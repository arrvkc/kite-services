import subprocess

js = r"""
(() => {
  const items = [];
  document.querySelectorAll('button, a, select, option, input, div, span, li').forEach((el, i) => {
    const text = (el.innerText || el.value || el.textContent || '').trim().replace(/\s+/g, ' ');
    if (
      text.includes('Eajee') ||
      text.includes('MOJOSCORE') ||
      text.includes('Overview') ||
      text.includes('Score') ||
      text.includes('Risk')
    ) {
      items.push({
        i,
        tag: el.tagName,
        text,
        id: el.id || '',
        cls: el.className || '',
        name: el.getAttribute('name') || '',
        href: el.getAttribute('href') || ''
      });
    }
  });
  return JSON.stringify(items, null, 2);
})()
"""

script = f'''
tell application "Google Chrome"
    set resultText to execute active tab of front window javascript {js!r}
end tell
return resultText
'''

out = subprocess.check_output(["osascript", "-e", script]).decode("utf-8")
print(out)
