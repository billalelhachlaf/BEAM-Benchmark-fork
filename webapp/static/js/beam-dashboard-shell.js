(function () {
  function q(sel) { return document.querySelector(sel); }
  function qa(sel) { return Array.from(document.querySelectorAll(sel)); }

  function applyViewMode() {
    const body = document.body;
    if (!body) return;
    const view = String(body.dataset.appView || 'create').toLowerCase();
    const showCreate = view === 'create';
    const showJobs = view === 'jobs';
    const showHistory = view === 'history';

    const create = q('#create-section');
    const jobs = q('#jobs-section');
    const history = q('#history-section');
    if (create) create.style.display = showCreate ? '' : 'none';
    if (jobs) jobs.style.display = showJobs ? '' : 'none';
    if (history) history.style.display = showHistory ? '' : 'none';

    qa('.app-nav-link[data-app-nav]').forEach((link) => {
      const key = String(link.dataset.appNav || '').toLowerCase();
      link.classList.toggle('active', key === view);
    });
  }

  function initWizard() {
    const form = q('#generate-form');
    if (!form) return;

    let step = 1;
    const max = 5;
    const panels = qa('.wizard-panel[data-step-panel]');
    const prevBtn = q('#wizard-prev-btn');
    const nextBtn = q('#wizard-next-btn');
    const submitBtn = q('#generate-form button[type="submit"]');
    const preflightBtn = q('#preflight-btn');
    const wizardErr = q('#wizard-error-box');

    function text(id) {
      const el = q(id);
      return el ? String(el.value || '').trim() : '';
    }

    function checked(id) {
      const el = q(id);
      return !!(el && el.checked);
    }

    function setError(msg) {
      if (!wizardErr) return;
      const m = String(msg || '').trim();
      wizardErr.textContent = m;
      wizardErr.classList.toggle('show', !!m);
    }

    function validateStep(idx) {
      if (idx === 1) {
        if (!text('#class-name-select')) return 'Step 1: select a class name.';
        if (!text('#matching-mode-select')) return 'Step 1: select a matching mode.';
      }
      if (idx === 2) {
        if (!text('#wdc-pattern-input')) return 'Step 2: add at least one mapping rule.';
      }
      if (idx === 3) {
        const endpoint = text('#target-endpoint-select');
        if (!endpoint) return 'Step 1: choose a target endpoint.';
        if (endpoint === 'custom' && !text('#target-endpoint-url-input')) {
          return 'Step 1: custom endpoint URL is required.';
        }
      }
      if (idx === 4) {
        if (!text('#parts-spec-input')) return 'Step 4: provide parts to process.';
      }
      if (idx === 5) {
        if (!text('#class-name-select') || !text('#wdc-pattern-input')) {
          return 'Step 5: configuration incomplete. Complete previous steps.';
        }
      }
      return '';
    }

    function render() {
      panels.forEach((panel) => {
        const idx = Number(panel.dataset.stepPanel || 0);
        panel.style.display = idx === step ? '' : 'none';
      });
      qa('.wizard-step[data-step-nav]').forEach((el) => {
        const idx = Number(el.dataset.stepNav || 0);
        el.classList.toggle('active', idx === step);
        el.classList.toggle('done', idx < step);
      });
      if (prevBtn) prevBtn.style.display = step > 1 ? '' : 'none';
      if (nextBtn) nextBtn.style.display = step < max ? '' : 'none';
      if (submitBtn) submitBtn.style.display = step === max ? '' : 'none';
      if (preflightBtn) preflightBtn.style.display = step === max ? '' : 'none';
      setError('');
    }

    if (prevBtn) {
      prevBtn.addEventListener('click', function () {
        step = Math.max(1, step - 1);
        render();
      });
    }
    if (nextBtn) {
      nextBtn.addEventListener('click', function () {
        const err = validateStep(step);
        if (err) {
          setError(err);
          return;
        }
        step = Math.min(max, step + 1);
        render();
      });
    }
    qa('.wizard-step[data-step-nav]').forEach((el) => {
      el.addEventListener('click', function () {
        const idx = Number(el.dataset.stepNav || 0);
        if (idx >= 1 && idx <= max) {
          const err = idx > step ? validateStep(step) : '';
          if (err) {
            setError(err);
            return;
          }
          step = idx;
          render();
        }
      });
    });

    form.addEventListener('submit', function (event) {
      const err = validateStep(5);
      if (err) {
        event.preventDefault();
        setError(err);
      }
    });

    render();
  }

  document.addEventListener('DOMContentLoaded', function () {
    applyViewMode();
    initWizard();
  });
})();
