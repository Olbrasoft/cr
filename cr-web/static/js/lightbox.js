/**
 * Lightbox Gallery + Right-column Slideshow — vanilla JS.
 *
 * Gallery data: [data-gallery-photos] JSON array on the source element.
 * Open triggers: [data-gallery-open="group"] with [data-index="N"].
 *
 * Slideshow: [data-slideshow="group"] with same data-gallery-photos.
 * Controlled by .slide-prev / .slide-next buttons.
 */
(function () {
    'use strict';

    var overlay, mainImg, spinner, prevBtn, nextBtn, closeBtn, thumbStrip, counter;
    var photos = [];
    var currentIndex = 0;
    var touchStartX = 0;

    function createOverlay() {
        overlay = document.createElement('div');
        overlay.className = 'lightbox-overlay';

        // Structure: close btn, then [prev] [main+spinner] [next], then thumbs
        overlay.innerHTML =
            '<button class="lightbox-close" aria-label="Zavřít">&times;</button>' +
            '<div class="lightbox-content">' +
                '<button class="lightbox-nav lightbox-prev" aria-label="Předchozí">&#8249;</button>' +
                '<div class="lightbox-main">' +
                    '<div class="lightbox-spinner"></div>' +
                    '<img alt="">' +
                '</div>' +
                '<button class="lightbox-nav lightbox-next" aria-label="Další">&#8250;</button>' +
            '</div>' +
            '<div class="lightbox-thumbs"></div>';

        prevBtn = overlay.querySelector('.lightbox-prev');
        nextBtn = overlay.querySelector('.lightbox-next');
        mainImg = overlay.querySelector('.lightbox-main img');
        spinner = overlay.querySelector('.lightbox-spinner');
        closeBtn = overlay.querySelector('.lightbox-close');
        thumbStrip = overlay.querySelector('.lightbox-thumbs');

        counter = document.createElement('div');
        counter.className = 'lightbox-counter';
        overlay.querySelector('.lightbox-main').appendChild(counter);

        document.body.appendChild(overlay);

        closeBtn.addEventListener('click', close);
        prevBtn.addEventListener('click', function (e) { e.stopPropagation(); prev(); });
        nextBtn.addEventListener('click', function (e) { e.stopPropagation(); next(); });

        // Only close on close button — NOT on overlay click
        // (user requested: close only with X button)

        // Keyboard
        document.addEventListener('keydown', function (e) {
            if (!overlay.classList.contains('open')) return;
            if (e.key === 'Escape') close();
            else if (e.key === 'ArrowLeft') prev();
            else if (e.key === 'ArrowRight') next();
        });

        // Touch swipe on main image area
        var content = overlay.querySelector('.lightbox-content');
        content.addEventListener('touchstart', function (e) {
            touchStartX = e.changedTouches[0].screenX;
        }, { passive: true });

        content.addEventListener('touchend', function (e) {
            var diff = touchStartX - e.changedTouches[0].screenX;
            if (Math.abs(diff) > 50) {
                if (diff > 0) next();
                else prev();
            }
        }, { passive: true });
    }

    function open(group, index) {
        if (!overlay) createOverlay();

        var source = document.querySelector('[data-gallery-photos][data-gallery-open="' + group + '"]');
        if (!source) {
            source = document.querySelector('[data-gallery-photos][data-slideshow="' + group + '"]');
        }
        if (!source) return;

        try {
            photos = JSON.parse(source.getAttribute('data-gallery-photos'));
        } catch (e) {
            return;
        }
        if (!photos || photos.length === 0) return;

        currentIndex = index || 0;
        if (currentIndex >= photos.length) currentIndex = 0;

        buildThumbs();
        showPhoto(currentIndex);

        overlay.classList.add('open');
        document.body.style.overflow = 'hidden';
        updateNav();
    }

    function close() {
        if (!overlay) return;
        overlay.classList.remove('open');
        document.body.style.overflow = '';
        mainImg.src = '';
        mainImg.classList.remove('loaded');
    }

    function prev() {
        if (currentIndex > 0) showPhoto(currentIndex - 1);
    }

    function next() {
        if (currentIndex < photos.length - 1) showPhoto(currentIndex + 1);
    }

    function showPhoto(index) {
        currentIndex = index;
        mainImg.classList.remove('loaded');
        spinner.style.display = '';

        var img = new Image();
        img.onload = function () {
            mainImg.src = img.src;
            mainImg.classList.add('loaded');
            spinner.style.display = 'none';
        };
        img.onerror = function () {
            spinner.style.display = 'none';
        };
        img.src = photos[index];

        var thumbs = thumbStrip.querySelectorAll('.lightbox-thumb');
        for (var i = 0; i < thumbs.length; i++) {
            thumbs[i].classList.toggle('active', i === index);
        }

        if (index > 0) { var p = new Image(); p.src = photos[index - 1]; }
        if (index < photos.length - 1) { var n = new Image(); n.src = photos[index + 1]; }

        updateNav();
        counter.textContent = (index + 1) + ' / ' + photos.length;
    }

    function updateNav() {
        if (photos.length <= 1) {
            prevBtn.style.display = 'none';
            nextBtn.style.display = 'none';
            counter.style.display = 'none';
        } else {
            prevBtn.style.display = currentIndex > 0 ? '' : 'none';
            nextBtn.style.display = currentIndex < photos.length - 1 ? '' : 'none';
            counter.style.display = '';
        }
    }

    function buildThumbs() {
        thumbStrip.innerHTML = '';
        if (photos.length <= 1) return;

        for (var i = 0; i < photos.length; i++) {
            var thumb = document.createElement('div');
            thumb.className = 'lightbox-thumb' + (i === currentIndex ? ' active' : '');
            thumb.innerHTML = '<img src="' + photos[i] + '" alt="">';
            thumb.setAttribute('data-index', i);
            thumb.addEventListener('click', function () {
                showPhoto(parseInt(this.getAttribute('data-index')));
            });
            thumbStrip.appendChild(thumb);
        }
    }

    // Click on [data-gallery-open] opens lightbox
    document.addEventListener('click', function (e) {
        var target = e.target.closest('[data-gallery-open]');
        if (!target) return;
        e.preventDefault();
        var group = target.getAttribute('data-gallery-open');
        var index = parseInt(target.getAttribute('data-index') || '0');
        open(group, index);
    });

    // Ensure body overflow is reset on page show (back/forward cache, new tab)
    window.addEventListener('pageshow', function () {
        if (!overlay || !overlay.classList.contains('open')) {
            document.body.style.overflow = '';
        }
    });

    // --- Right-column slideshow ---
    document.addEventListener('DOMContentLoaded', function () {
        var slideshows = document.querySelectorAll('[data-slideshow]');
        for (var s = 0; s < slideshows.length; s++) {
            initSlideshow(slideshows[s]);
        }
    });

    function initSlideshow(el) {
        var group = el.getAttribute('data-slideshow');
        var photosJson, thumbsJson;
        try {
            photosJson = JSON.parse(el.getAttribute('data-gallery-photos'));
            thumbsJson = JSON.parse(el.getAttribute('data-gallery-thumbs') || '[]');
        } catch (e) {
            return;
        }
        if (!photosJson || photosJson.length === 0) return;
        // Use thumbs for slide display, full URLs for lightbox
        if (!thumbsJson || thumbsJson.length === 0) thumbsJson = photosJson;

        var startIndex = parseInt(el.getAttribute('data-slide-start') || '0');
        var currentSlide = startIndex;
        var img = el.querySelector('img');
        var counterEl = el.querySelector('.slide-counter');
        var prevEl = el.querySelector('.slide-prev');
        var nextEl = el.querySelector('.slide-next');

        function show(idx) {
            currentSlide = idx;
            img.src = thumbsJson[idx];
            img.setAttribute('data-index', idx);
            if (counterEl) {
                counterEl.textContent = (idx + 1) + ' / ' + photosJson.length;
            }
            if (prevEl) prevEl.style.display = idx > 0 ? '' : 'none';
            if (nextEl) nextEl.style.display = idx < photosJson.length - 1 ? '' : 'none';
        }

        if (prevEl) {
            prevEl.addEventListener('click', function (e) {
                e.stopPropagation();
                if (currentSlide > 0) show(currentSlide - 1);
            });
        }
        if (nextEl) {
            nextEl.addEventListener('click', function (e) {
                e.stopPropagation();
                if (currentSlide < photosJson.length - 1) show(currentSlide + 1);
            });
        }

        // Click on image (not on arrows) opens lightbox at current slide
        img.addEventListener('click', function () {
            open(group, currentSlide);
        });

        show(currentSlide);
    }
})();
