// THIS FILE IS INTENTIONALLY BROKEN.
// It exists ONLY to verify that the FIFO push-wake mechanism delivers a
// CI failure event back to the originating Claude Code session.
// The PR containing this file MUST NOT be merged — close it after the
// failure wake event has been received.

pub fn intentional_negative_test_for_ci_wake() -> i32 {
    let value: i32 = "this is not a number";
    value
}
