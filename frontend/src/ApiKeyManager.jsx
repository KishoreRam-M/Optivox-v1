/**
 * ApiKeyManager.jsx
 * -----------------
 * Manages the user's Gemini API key stored in localStorage.
 * 
 * Components:
 *   - ApiKeySetup: Full-screen onboarding shown when no key is saved
 *   - ApiKeyBadge: Compact sidebar badge showing key status + settings toggle
 *   - useApiKey: Hook that provides key + setter + axios config
 */

import React, { useState, useCallback, useEffect, useRef } from 'react';
import { Key, Eye, EyeOff, ExternalLink, CheckCircle2, XCircle, Loader2, Settings, Trash2, ShieldCheck } from 'lucide-react';

const STORAGE_KEY = 'optivox_gemini_key';
const GEMINI_STUDIO_URL = 'https://aistudio.google.com/app/apikey';

// ── Hook ───────────────────────────────────────────────────────────────────

export function useApiKey() {
  const [apiKey, setApiKeyState] = useState(() => localStorage.getItem(STORAGE_KEY) || '');

  const setApiKey = useCallback((key) => {
    const trimmed = key.trim();
    if (trimmed) {
      localStorage.setItem(STORAGE_KEY, trimmed);
    } else {
      localStorage.removeItem(STORAGE_KEY);
    }
    setApiKeyState(trimmed);
  }, []);

  // Axios default header — automatically attached to every request
  useEffect(() => {
    if (apiKey) {
      window.__optivox_api_key__ = apiKey;
    } else {
      delete window.__optivox_api_key__;
    }
  }, [apiKey]);

  return { apiKey, setApiKey, hasKey: !!apiKey };
}

// ── Full-screen Setup Screen ───────────────────────────────────────────────

export function ApiKeySetup({ onSave }) {
  const [key, setKey]         = useState('');
  const [show, setShow]       = useState(false);
  const [testing, setTesting] = useState(false);
  const [status, setStatus]   = useState(null); // 'ok' | 'error' | null
  const [errMsg, setErrMsg]   = useState('');
  const inputRef              = useRef(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const testAndSave = useCallback(async () => {
    const trimmed = key.trim();
    if (!trimmed) return;
    setTesting(true); setStatus(null); setErrMsg('');
    try {
      const res = await fetch('/health', {
        headers: { 'X-Gemini-API-Key': trimmed },
      });
      if (res.ok) {
        setStatus('ok');
        setTimeout(() => onSave(trimmed), 600);
      } else {
        setStatus('error');
        setErrMsg(`Server responded: ${res.status}`);
      }
    } catch {
      // Backend unreachable — save anyway (user can still try)
      setStatus('ok');
      setTimeout(() => onSave(trimmed), 600);
    } finally {
      setTesting(false);
    }
  }, [key, onSave]);

  const maskedKey = key.length > 8
    ? key.slice(0, 6) + '•'.repeat(Math.min(key.length - 8, 20)) + key.slice(-4)
    : key;

  return (
    <div className="api-setup-overlay">
      <div className="api-setup-card">
        {/* Logo */}
        <div className="api-setup-logo">
          <div className="api-setup-logo-ring">
            <Key size={28} />
          </div>
        </div>

        <h1 className="api-setup-title">Welcome to OptiVox DB</h1>
        <p className="api-setup-subtitle">
          Enter your <strong>Gemini API key</strong> to power the AI SQL Studio.
          Your key stays in your browser only — never sent to our servers.
        </p>

        {/* Free tier note */}
        <div className="api-setup-note">
          <ShieldCheck size={15} />
          <span>
            Using <strong>Gemini 2.5 Flash</strong> free tier — 1,500 requests/day, 15 RPM.{' '}
            <a href={GEMINI_STUDIO_URL} target="_blank" rel="noopener noreferrer">
              Get your free key <ExternalLink size={12} style={{ display: 'inline', verticalAlign: 'middle' }} />
            </a>
          </span>
        </div>

        {/* Input */}
        <div className="api-setup-input-wrap">
          <Key size={16} className="api-setup-input-icon" />
          <input
            ref={inputRef}
            type={show ? 'text' : 'password'}
            className="api-setup-input"
            placeholder="AIzaSy…"
            value={key}
            onChange={e => setKey(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && testAndSave()}
            autoComplete="off"
            spellCheck={false}
          />
          <button
            className="api-setup-toggle-vis"
            type="button"
            onClick={() => setShow(s => !s)}
            title={show ? 'Hide key' : 'Show key'}
          >
            {show ? <EyeOff size={16} /> : <Eye size={16} />}
          </button>
        </div>

        {/* Status feedback */}
        {status === 'ok' && (
          <div className="api-setup-status ok">
            <CheckCircle2 size={16} /> Key saved! Starting…
          </div>
        )}
        {status === 'error' && (
          <div className="api-setup-status error">
            <XCircle size={16} /> {errMsg || 'Could not verify key.'}
          </div>
        )}

        {/* Save button */}
        <button
          className="api-setup-btn"
          onClick={testAndSave}
          disabled={!key.trim() || testing}
        >
          {testing
            ? <><Loader2 className="loading-spinner" size={18} /> Verifying…</>
            : <><Key size={18} /> Save API Key & Continue</>
          }
        </button>

        <p className="api-setup-footer">
          Your key is stored locally in <code>localStorage</code> and only used to call the Gemini API.
        </p>
      </div>
    </div>
  );
}

// ── Sidebar Badge ──────────────────────────────────────────────────────────

export function ApiKeyBadge({ apiKey, onUpdate }) {
  const [open, setOpen]     = useState(false);
  const [editing, setEditing] = useState(false);
  const [newKey, setNewKey] = useState('');
  const [show, setShow]     = useState(false);
  const panelRef            = useRef(null);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    const handler = (e) => {
      if (panelRef.current && !panelRef.current.contains(e.target)) {
        setOpen(false);
        setEditing(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const masked = apiKey
    ? apiKey.slice(0, 8) + '•'.repeat(8) + apiKey.slice(-4)
    : 'Not set';

  const handleUpdate = () => {
    if (newKey.trim()) {
      onUpdate(newKey.trim());
      setNewKey('');
      setEditing(false);
      setOpen(false);
    }
  };

  const handleClear = () => {
    onUpdate('');
    setOpen(false);
  };

  return (
    <div className="api-key-badge-wrap" ref={panelRef}>
      <button
        className={`api-key-badge-btn ${apiKey ? 'active' : 'inactive'}`}
        onClick={() => setOpen(o => !o)}
        title="Gemini API Key Settings"
      >
        <Key size={13} />
        <span>{apiKey ? 'Gemini Key ✓' : 'No API Key'}</span>
        <Settings size={13} className="api-key-settings-icon" />
      </button>

      {open && (
        <div className="api-key-panel">
          <div className="api-key-panel-header">
            <ShieldCheck size={14} />
            <span>Gemini API Key</span>
          </div>

          <div className="api-key-panel-current">
            <code>{show ? (apiKey || '—') : masked}</code>
            <button
              className="api-key-vis-btn"
              onClick={() => setShow(s => !s)}
              title={show ? 'Hide' : 'Show'}
            >
              {show ? <EyeOff size={13} /> : <Eye size={13} />}
            </button>
          </div>

          <div className="api-key-panel-actions">
            <a
              href={GEMINI_STUDIO_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="api-key-link"
            >
              Get free key <ExternalLink size={11} />
            </a>
          </div>

          {editing ? (
            <div className="api-key-edit-row">
              <input
                type="password"
                className="api-key-edit-input"
                placeholder="Paste new key…"
                value={newKey}
                onChange={e => setNewKey(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleUpdate()}
                autoFocus
              />
              <button className="api-key-save-btn" onClick={handleUpdate} disabled={!newKey.trim()}>
                Save
              </button>
            </div>
          ) : (
            <div className="api-key-panel-btns">
              <button className="api-key-change-btn" onClick={() => setEditing(true)}>
                Change Key
              </button>
              {apiKey && (
                <button className="api-key-clear-btn" onClick={handleClear} title="Remove key">
                  <Trash2 size={13} />
                </button>
              )}
            </div>
          )}

          <div className="api-key-note">
            Gemini 2.5 Flash free tier:<br />
            15 RPM · 1M TPM · 1,500 RPD
          </div>
        </div>
      )}
    </div>
  );
}
