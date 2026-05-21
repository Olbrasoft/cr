#!/usr/bin/env node
// Fetch ČSFD audience ratings for rows that already have csfd_id and store
// them in films.csfd_rating / .csfd_rating_count / .csfd_rating_synced_at
// (and the matching columns on series + tv_shows).
//
// Background:
//   * ČSFD has no public API. The schema.org JSON-LD on each film page
//     carries aggregateRating.ratingValue + ratingCount.
//   * ČSFD now sits behind Anubis proof-of-work anti-bot — plain
//     curl/requests get a challenge page back. The
//     bartholomej/node-csfd-api npm package (52★, weekly releases) sends
//     a rotating browser fingerprint and passes the challenge transparently.
//   * Spike on 1000 prod rows (2026-05-20): 35.4 req/s sustained with
//     concurrency=8, zero errors, no rate-limit signal. Full refresh of
//     38 k rows ≈ 18 min.
//
// Usage:
//   node fetch_csfd_ratings.mjs --table films|series|tv_shows|all
//                               [--limit N]         # default 5000
//                               [--concurrency C]   # default 8
//                               [--max-age-days D]  # default 7
//                               [--dry-run]
//
// Picker query (per table):
//   SELECT id, csfd_id FROM <table>
//    WHERE csfd_id IS NOT NULL
//      AND (csfd_rating_synced_at IS NULL
//           OR csfd_rating_synced_at < now() - INTERVAL '<D> days')
//    ORDER BY csfd_rating_synced_at NULLS FIRST
//    LIMIT N;
//
// Refresh policy:
//   * On success: UPDATE csfd_rating, csfd_rating_count, csfd_rating_synced_at.
//   * When ČSFD returns no rating (rating=null, 0 hodnotitelů): still stamp
//     csfd_rating_synced_at=now() and leave rating=NULL / count=0 — otherwise
//     we'd re-fetch the same zero-rated film every day.
//   * On error (network / 404 / Anubis): leave the row untouched so the
//     next run retries.

import { csfd } from "node-csfd-api";
import pg from "pg";
import pLimit from "p-limit";
import fs from "node:fs";

const TABLES = ["films", "series", "tv_shows"];

function parseArgs(argv) {
  const args = {
    table: null,
    limit: 5000,
    concurrency: 8,
    maxAgeDays: 7,
    dryRun: false,
    tsvOut: null,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a === "--table") args.table = next();
    else if (a === "--limit") args.limit = parseInt(next(), 10);
    else if (a === "--concurrency") args.concurrency = parseInt(next(), 10);
    else if (a === "--max-age-days") args.maxAgeDays = parseInt(next(), 10);
    else if (a === "--dry-run") args.dryRun = true;
    else if (a === "--tsv-out") args.tsvOut = next();
    else if (a === "--help" || a === "-h") {
      console.log(
        "Usage: node fetch_csfd_ratings.mjs --table films|series|tv_shows|all\n" +
          "                                  [--limit N=5000] [--concurrency C=8]\n" +
          "                                  [--max-age-days D=7] [--dry-run]\n" +
          "                                  [--tsv-out path]\n",
      );
      process.exit(0);
    } else {
      throw new Error(`unknown arg: ${a}`);
    }
  }
  if (!args.table) throw new Error("--table is required (films|series|tv_shows|all)");
  if (args.table !== "all" && !TABLES.includes(args.table))
    throw new Error(`--table must be one of: ${TABLES.join(", ")}, all`);
  if (!Number.isFinite(args.limit) || args.limit <= 0)
    throw new Error("--limit must be a positive integer");
  if (!Number.isFinite(args.concurrency) || args.concurrency <= 0)
    throw new Error("--concurrency must be a positive integer");
  if (!Number.isFinite(args.maxAgeDays) || args.maxAgeDays < 0)
    throw new Error("--max-age-days must be a non-negative integer");
  return args;
}

// Per-row TSV log so a debug run can produce a machine-readable trace
// alongside the human-readable stdout. Default routing for systemd is
// stdout → /var/log/cr-csfd-ratings.log (configured by #762's unit
// file); the TSV is opt-in via --tsv-out and is meant for ad-hoc local
// debugging and the one-shot initial backfill (#763).
let tsvStream = null;
function openTsv(path) {
  if (!path) return;
  try {
    tsvStream = fs.createWriteStream(path, { flags: "a" });
    tsvStream.on("error", (e) => {
      console.error(`tsv-out: write error on ${path}: ${e.message} — falling back to stdout only`);
      tsvStream = null;
    });
    tsvStream.write("ts\ttable\tid\tcsfd_id\trating\tcount\tstatus\n");
  } catch (e) {
    console.error(`tsv-out: cannot open ${path}: ${e.message} — falling back to stdout only`);
    tsvStream = null;
  }
}
function writeTsv(cols) {
  if (!tsvStream) return;
  tsvStream.write([new Date().toISOString(), ...cols].join("\t") + "\n");
}

async function pickRows(pool, table, limit, maxAgeDays) {
  const { rows } = await pool.query(
    `SELECT id, csfd_id FROM ${table}
      WHERE csfd_id IS NOT NULL
        AND (csfd_rating_synced_at IS NULL
             OR csfd_rating_synced_at < now() - ($1::text || ' days')::interval)
      ORDER BY csfd_rating_synced_at NULLS FIRST
      LIMIT $2`,
    [String(maxAgeDays), limit],
  );
  return rows;
}

// node-csfd-api returns { rating: 77, ratingCount: 12, ... } for movies.
// For series/tv_shows the API path is csfd.movie() too — ČSFD's URL space
// uses /film/{id} regardless of TV vs. cinema, and the lib follows.
async function fetchOne(csfdId) {
  const m = await csfd.movie(csfdId);
  return {
    rating: typeof m.rating === "number" ? Math.round(m.rating) : null,
    ratingCount: typeof m.ratingCount === "number" ? m.ratingCount : 0,
  };
}

async function processTable(pool, table, args) {
  const rows = await pickRows(pool, table, args.limit, args.maxAgeDays);
  console.log(`[${table}] picked ${rows.length} rows (limit=${args.limit}, max-age=${args.maxAgeDays}d)`);
  if (rows.length === 0) return { table, picked: 0, ok: 0, err: 0, withRating: 0 };

  const limit = pLimit(args.concurrency);
  const stats = { table, picked: rows.length, ok: 0, err: 0, withRating: 0, applied: 0 };
  const t0 = Date.now();
  const errors = [];

  const tasks = rows.map((row) =>
    limit(async () => {
      let r = null;
      try {
        r = await fetchOne(row.csfd_id);
        if (args.dryRun) {
          if (stats.ok < 5)
            console.log(`  DRY id=${row.id} csfd=${row.csfd_id} → rating=${r.rating} count=${r.ratingCount}`);
        } else {
          // The UPDATE is unguarded — last writer wins. If a parallel
          // worker stamped this row between SELECT and UPDATE we'd just
          // overwrite with a fresher value, which is a no-op for
          // correctness. We do NOT touch csfd_id.
          const res = await pool.query(
            `UPDATE ${table}
                SET csfd_rating = $1,
                    csfd_rating_count = $2,
                    csfd_rating_synced_at = now()
              WHERE id = $3`,
            [r.rating, r.ratingCount, row.id],
          );
          if (res.rowCount > 0) stats.applied++;
        }
        // Only count a row as ok once both fetch AND (dry-run or UPDATE)
        // succeeded — Copilot review on PR #768 caught the previous code
        // double-counting rows whose fetch passed but UPDATE threw
        // (both ok++ and err++ ran for the same row).
        stats.ok++;
        if (r.rating != null) stats.withRating++;
        writeTsv([table, row.id, row.csfd_id, r.rating ?? "", r.ratingCount, "ok"]);
      } catch (e) {
        stats.err++;
        errors.push({ id: row.id, csfd_id: row.csfd_id, msg: e.message.slice(0, 200) });
        writeTsv([table, row.id, row.csfd_id, "", "", "err:" + e.message.slice(0, 80).replace(/\t/g, " ")]);
      }
      if ((stats.ok + stats.err) % 100 === 0) {
        const elapsed = (Date.now() - t0) / 1000;
        const rate = stats.ok / Math.max(elapsed, 0.001);
        console.log(
          `  [${table}] ${stats.ok + stats.err}/${rows.length}  ok=${stats.ok} err=${stats.err}  ${rate.toFixed(1)} req/s`,
        );
      }
    }),
  );
  await Promise.all(tasks);

  const wallMs = Date.now() - t0;
  console.log(
    `[${table}] DONE in ${(wallMs / 1000).toFixed(1)}s — ok=${stats.ok} err=${stats.err}` +
      ` applied=${stats.applied} with_rating=${stats.withRating}` +
      ` (${(stats.ok / (wallMs / 1000)).toFixed(1)} req/s)`,
  );
  if (errors.length) {
    const sample = errors.slice(0, 5);
    console.log(`[${table}] first ${sample.length} errors:`);
    for (const e of sample) console.log(`  id=${e.id} csfd=${e.csfd_id}: ${e.msg}`);
  }
  return stats;
}

async function main() {
  const args = parseArgs(process.argv);
  console.log(
    `cr-csfd-ratings: table=${args.table} limit=${args.limit} conc=${args.concurrency}` +
      ` max-age=${args.maxAgeDays}d dry-run=${args.dryRun}` +
      (args.tsvOut ? ` tsv-out=${args.tsvOut}` : ""),
  );

  if (!process.env.DATABASE_URL) {
    console.error("ERROR: DATABASE_URL not set");
    process.exit(2);
  }

  openTsv(args.tsvOut);

  // Pool sized to match worker concurrency + 1 spare for the picker SELECT.
  // pg.Client (single connection) would serialise all queries through one
  // socket and emit a deprecation warning when concurrent UPDATEs queue up.
  const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
    max: args.concurrency + 1,
  });

  const targets = args.table === "all" ? TABLES : [args.table];
  let totalErr = 0;
  try {
    for (const t of targets) {
      const s = await processTable(pool, t, args);
      totalErr += s.err;
    }
  } finally {
    await pool.end();
    if (tsvStream) {
      await new Promise((resolve) => tsvStream.end(resolve));
    }
  }
  process.exit(totalErr > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(2);
});
