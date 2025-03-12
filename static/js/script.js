// Wait for DOM to be fully loaded before initializing
document.addEventListener('DOMContentLoaded', function() {
    // Initialize animation library
    AOS.init({
        duration: 500,
        easing: 'ease-in-out',
        once: true,
        mirror: false
    });
    
    // Initialize EmailJS
    initEmailJS();
    
    // Initialize all components
    initContactForm();
    initCounters();
    initLightbox();
    initSmoothScrolling();
    initBackToTop();
    initStickyHeader();
    initMobileMenu();
    initAnimations();
    initTestimonialCarousel();
    initHoverDropdowns();
});

/**
 * Initialize EmailJS service
 */
function initEmailJS() {
    try {
        // Replace with your actual EmailJS user ID
        emailjs.init("YOUR_USER_ID_HERE");
    } catch (error) {
        console.error('EmailJS initialization failed:', error);
    }
}

/**
 * Initialize contact form with validation and submission handling
 */
function initContactForm() {
    const contactForm = document.getElementById('contactForm');
    if (!contactForm) return;
    
    contactForm.addEventListener('submit', async function(event) {
        event.preventDefault();
        
        // Basic validation before submission
        if (!validateForm(contactForm)) return;
        
        // Show loading state
        const submitButton = contactForm.querySelector('button[type="submit"]');
        const originalButtonText = submitButton.innerHTML;
        setButtonLoading(submitButton, true);
        
        // Get form data as an object
        const formData = {
            name: document.getElementById('name').value.trim(),
            email: document.getElementById('email').value.trim(),
            phone: document.getElementById('phone').value.trim(),
            inquiry: document.getElementById('inquiry').value,
            message: document.getElementById('message').value.trim()
        };
        
        try {
            // Send email using EmailJS
            const response = await emailjs.send('default_service', 'contact_form', formData);
            showFormResponse(true, 'Your message has been sent successfully! We will get back to you soon.');
            contactForm.reset();
        } catch (error) {
            console.error('EmailJS error:', error);
            showFormResponse(false, 'Sorry, there was an error sending your message. Please try again later.');
        } finally {
            // Reset button state
            setButtonLoading(submitButton, false, originalButtonText);
            
            // Scroll to response message
            const responseDiv = document.getElementById('formResponse');
            if (responseDiv) {
                responseDiv.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                
                // Auto-hide message after delay
                setTimeout(() => {
                    responseDiv.style.display = 'none';
                }, 5000);
            }
        }
    });
}

/**
 * Basic form validation
 * @param {HTMLFormElement} form - The form to validate
 * @returns {boolean} - Whether the form is valid
 */
function validateForm(form) {
    let isValid = true;
    const requiredFields = form.querySelectorAll('[required]');
    
    requiredFields.forEach(field => {
        if (!field.value.trim()) {
            isValid = false;
            field.classList.add('is-invalid');
        } else {
            field.classList.remove('is-invalid');
        }
    });
    
    // Email validation
    const emailField = form.querySelector('input[type="email"]');
    if (emailField && emailField.value.trim()) {
        const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailPattern.test(emailField.value.trim())) {
            emailField.classList.add('is-invalid');
            isValid = false;
        }
    }
    
    return isValid;
}

/**
 * Set button loading state
 * @param {HTMLButtonElement} button - The button element
 * @param {boolean} isLoading - Whether to show loading state
 * @param {string} originalText - Original button text to restore
 */
function setButtonLoading(button, isLoading, originalText = null) {
    if (isLoading) {
        button.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Sending...';
        button.disabled = true;
    } else {
        button.innerHTML = originalText;
        button.disabled = false;
    }
}

/**
 * Show form response message
 * @param {boolean} success - Whether the submission was successful
 * @param {string} message - Message to display
 */
function showFormResponse(success, message) {
    const responseDiv = document.getElementById('formResponse');
    if (!responseDiv) return;
    
    const icon = success ? 'check-circle' : 'exclamation-circle';
    const alertClass = success ? 'alert-success' : 'alert-danger';
    
    responseDiv.innerHTML = `<div class="alert ${alertClass}"><i class="fas fa-${icon} me-2"></i>${message}</div>`;
    responseDiv.style.display = 'block';
}

/**
 * Initialize counters with Intersection Observer
 */
function initCounters() {
    const counters = document.querySelectorAll('.counter');
    if (counters.length === 0) return;
    
    const speed = 200; // Speed of counting animation
    
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const counter = entry.target;
                const target = parseInt(counter.getAttribute('data-count') || '0');
                animateCounter(counter, target, speed);
                observer.unobserve(counter);
            }
        });
    }, { threshold: 0.5 });
    
    counters.forEach(counter => observer.observe(counter));
}

/**
 * Animate a counter from 0 to target
 * @param {HTMLElement} counter - The counter element
 * @param {number} target - Target value
 * @param {number} speed - Animation speed
 */
function animateCounter(counter, target, speed) {
    let count = 0;
    const increment = target / speed;
    
    const updateCount = () => {
        if (count < target) {
            count += increment;
            counter.innerText = Math.ceil(count);
            requestAnimationFrame(updateCount);
        } else {
            counter.innerText = target;
        }
    };
    
    requestAnimationFrame(updateCount);
}

/**
 * Initialize lightbox for gallery images
 */
function initLightbox() {
    if (typeof lightbox === 'undefined') return;
    
    lightbox.option({
        'resizeDuration': 200,
        'wrapAround': true,
        'albumLabel': 'Image %1 of %2',
        'fadeDuration': 300
    });
}

/**
 * Initialize smooth scrolling for anchor links
 */
function initSmoothScrolling() {
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            // Skip if it's a dropdown toggle
            if (this.classList.contains('dropdown-toggle')) return;
            
            const targetId = this.getAttribute('href');
            if (targetId === '#') return; // Skip for "#" links
            
            e.preventDefault();
            
            const targetElement = document.querySelector(targetId);
            if (targetElement) {
                targetElement.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
                
                // Update URL hash without page jump
                history.pushState(null, null, targetId);
            }
        });
    });
}

/**
 * Initialize back-to-top button functionality
 */
function initBackToTop() {
    const backToTopButton = document.querySelector('.back-to-top');
    if (!backToTopButton) return;
    
    // Use a debounced scroll listener for better performance
    const onScroll = debounce(() => {
        backToTopButton.classList.toggle('show', window.pageYOffset > 300);
    }, 50);
    
    window.addEventListener('scroll', onScroll);
    
    backToTopButton.addEventListener('click', (e) => {
        e.preventDefault();
        window.scrollTo({ top: 0, behavior: 'smooth' });
    });
}

/**
 * Initialize sticky header on scroll
 */
function initStickyHeader() {
    const header = document.querySelector('.header');
    if (!header) return;
    
    // Use a debounced scroll listener
    const onScroll = debounce(() => {
        header.classList.toggle('sticky', window.pageYOffset > 100);
    }, 50);
    
    window.addEventListener('scroll', onScroll);
}

/**
 * Initialize mobile menu toggle and outside click handling
 */
function initMobileMenu() {
    const navbarToggler = document.querySelector('.navbar-toggler');
    const navbar = document.getElementById('navbarNav');
    if (!navbarToggler || !navbar) return;
    
    // Toggle menu
    navbarToggler.addEventListener('click', function() {
        document.body.classList.toggle('menu-open');
    });
    
    // Close mobile menu when clicking outside
    document.addEventListener('click', function(event) {
        const isNavbarExpanded = navbar.classList.contains('show');
        
        if (isNavbarExpanded && !navbar.contains(event.target) && !navbarToggler.contains(event.target)) {
            if (typeof bootstrap !== 'undefined') {
                const bsCollapse = new bootstrap.Collapse(navbar);
                bsCollapse.hide();
                document.body.classList.remove('menu-open');
            } else {
                // Fallback for when Bootstrap JS is not available
                navbar.classList.remove('show');
                document.body.classList.remove('menu-open');
            }
        }
    });
}

/**
 * Initialize animations for elements when they come into view
 */
function initAnimations() {
    const animatedElements = document.querySelectorAll('.fade-in, .slide-in, .scale-in');
    if (animatedElements.length === 0) return;
    
    const animationObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('visible');
                // Only observe once
                animationObserver.unobserve(entry.target);
            }
        });
    }, { threshold: 0.1, rootMargin: '0px 0px -50px 0px' });
    
    animatedElements.forEach(element => animationObserver.observe(element));
}

/**
 * Initialize testimonial carousel with Bootstrap
 */
function initTestimonialCarousel() {
    const testimonialCarousel = document.getElementById('testimonialCarousel');
    if (!testimonialCarousel || typeof bootstrap === 'undefined') return;
    
    try {
        const carousel = new bootstrap.Carousel(testimonialCarousel, {
            interval: 5000,
            wrap: true,
            touch: true
        });
    } catch (error) {
        console.error('Failed to initialize testimonial carousel:', error);
    }
}

/**
 * Initialize dropdown menus that activate on hover for desktop
 */
function initHoverDropdowns() {
    const dropdowns = document.querySelectorAll('.navbar .dropdown');
    if (dropdowns.length === 0) return;
    
    dropdowns.forEach(dropdown => {
        dropdown.addEventListener('mouseenter', handleDropdownHover);
        dropdown.addEventListener('mouseleave', handleDropdownHover);
    });
}

/**
 * Handle dropdown hover events
 * @param {Event} e - Mouse event
 */
function handleDropdownHover(e) {
    // Only apply hover behavior on desktop
    if (window.innerWidth <= 992) return;
    
    const dropdown = e.target.closest('.dropdown');
    const menu = dropdown.querySelector('.dropdown-menu');
    if (!menu) return;
    
    const isHovered = dropdown.matches(':hover');
    
    // Use setTimeout to prevent rapid toggling
    setTimeout(() => {
        menu.classList.toggle('show', isHovered);
        dropdown.classList.toggle('show', isHovered);
        
        if (isHovered) {
            menu.setAttribute('data-bs-popper', 'static');
        }
    }, 30);
}

/**
 * Simple debounce function to limit function calls
 * @param {Function} func - Function to debounce
 * @param {number} delay - Delay in milliseconds
 * @returns {Function} - Debounced function
 */
function debounce(func, delay) {
    let timeoutId;
    return function(...args) {
        clearTimeout(timeoutId);
        timeoutId = setTimeout(() => {
            func.apply(this, args);
        }, delay);
    };
}