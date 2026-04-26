import { useRef } from 'react';

/**
 * useSession — returns a stable, persistent session ID for the browser tab.
 *
 * - Stored in sessionStorage so it survives React re-renders and tab refreshes,
 *   but is scoped to the tab (a new tab gets a new session).
 * - Generated once as a random UUID-like string.
 */
const SESSION_KEY = 'optivox_session_id';

function generateSessionId() {
  // Crypto-random UUID when available, fallback to Math.random
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

export function getOrCreateSessionId() {
  let id = sessionStorage.getItem(SESSION_KEY);
  if (!id) {
    id = generateSessionId();
    sessionStorage.setItem(SESSION_KEY, id);
  }
  return id;
}

/**
 * useSession hook — returns a stable session_id ref.
 * Using a ref (not state) means it never triggers a re-render.
 */
export default function useSession() {
  const sessionIdRef = useRef(getOrCreateSessionId());
  return sessionIdRef.current;
}
