// daemon/notify.ts — Desktop notification abstraction
//
// Detects OS and dispatches to the appropriate notification tool.
// Fails silently if the notification tool is unavailable.

import { platform } from 'os';

/**
 * Send a desktop notification. Fails silently if the platform tool is
 * unavailable or the spawn fails.
 */
export function notify(title: string, body: string): void {
  const os = platform();

  try {
    if (os === 'darwin') {
      // macOS: osascript
      const script = `display notification ${JSON.stringify(body)} with title ${JSON.stringify(title)}`;
      Bun.spawn(['osascript', '-e', script], {
        stdio: ['ignore', 'ignore', 'ignore'],
      });
    } else if (os === 'linux') {
      // Linux: notify-send (libnotify)
      Bun.spawn(['notify-send', title, body], {
        stdio: ['ignore', 'ignore', 'ignore'],
      });
    }
    // Other platforms: no-op
  } catch {
    // Notification tool not available — ignore
  }
}
