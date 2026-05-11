(function () {
  function focusById(id) {
    const el = document.getElementById(id);
    if (el && typeof el.focus === 'function') {
      el.focus();
      if (typeof el.select === 'function') el.select();
    }
  }

  document.addEventListener('keydown', function (event) {
    const target = event.target;
    const tag = target && target.tagName ? target.tagName.toLowerCase() : '';
    const inInput = tag === 'input' || tag === 'textarea' || tag === 'select' || (target && target.isContentEditable);

    if (!inInput && event.key === '/') {
      event.preventDefault();
      focusById('history-search-input');
      return;
    }

    if (!inInput && (event.key === 'n' || event.key === 'N')) {
      event.preventDefault();
      focusById('class-name-select');
    }
  });
})();
