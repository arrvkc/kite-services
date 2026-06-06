import subprocess

js = r"""
(() => {
  const result = [];

  document.querySelectorAll('*').forEach(el => {
    const txt = (el.innerText || '').trim();

    if (
      txt === 'Eajee' ||
      txt === 'MOJOSCORE'
    ) {
      result.push({
        tag: el.tagName,
        text: txt,
        id: el.id,
        className: el.className,
        ngClick: el.getAttribute('ng-click'),
        ngRepeat: el.getAttribute('ng-repeat'),
        ngModel: el.getAttribute('ng-model'),
        href: el.getAttribute('href')
      });
    }
  });

  return JSON.stringify(result, null, 2);
})();
"""

script = f'''
tell application "Google Chrome"
    set resultText to execute active tab of front window javascript {js!r}
end tell
return resultText
'''

print(subprocess.check_output(["osascript", "-e", script]).decode())
