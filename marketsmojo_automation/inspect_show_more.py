from modules.chrome_applescript import run_js

js = r"""
(() => {
  const result = [];

  [...document.querySelectorAll('*')].forEach((el, i) => {
    const text = (el.innerText || '').trim();

    if (text === 'Show More') {
      const rect = el.getBoundingClientRect();
      const style = window.getComputedStyle(el);

      result.push({
        i,
        tag: el.tagName,
        text,
        id: el.id || '',
        className: el.className || '',
        ngClick: el.getAttribute('ng-click'),
        onclick: el.getAttribute('onclick'),
        href: el.getAttribute('href'),
        display: style.display,
        visibility: style.visibility,
        width: rect.width,
        height: rect.height
      });
    }
  });

  return JSON.stringify(result, null, 2);
})()
"""

print(run_js(js))
