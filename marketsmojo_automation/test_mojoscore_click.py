import subprocess

js = r"""
(() => {

  const mojo = [...document.querySelectorAll('a')]
      .find(a => a.innerText.trim() === 'MOJOSCORE');

  if (!mojo) {
      return 'MOJOSCORE NOT FOUND';
  }

  mojo.click();

  return 'MOJOSCORE CLICKED';
})();
"""

script = f'''
tell application "Google Chrome"
    set resultText to execute active tab of front window javascript {js!r}
end tell
return resultText
'''

print(subprocess.check_output(["osascript","-e",script]).decode())
