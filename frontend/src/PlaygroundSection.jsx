import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import {
  Trophy, Star, Lightbulb, Play, CheckCircle2, XCircle,
  ChevronRight, RotateCcw, Lock, Zap, BookOpen, TerminalSquare,
  Loader2, AlertCircle, Table2, Award
} from 'lucide-react';

const API = 'http://localhost:8000/api/playground';

const LEVEL_NAMES = {
  1: 'SELECT Basics',
  2: 'Filtering & Logic',
  3: 'Sorting & Limiting',
  4: 'Aggregations & Grouping',
  5: 'Multi-table JOINs',
  6: 'Subqueries & CTEs',
  7: 'Window Functions & Advanced',
};

const LEVEL_COLORS = {
  1: '#10b981',
  2: '#0ea5e9',
  3: '#3b82f6',
  4: '#f59e0b',
  5: '#8b5cf6',
  6: '#ef4444',
  7: '#ec4899',
};

export default function PlaygroundSection() {
  const [tasks, setTasks]           = useState([]);
  const [schema, setSchema]         = useState({});
  const [activeTask, setActiveTask] = useState(null);
  const [sql, setSql]               = useState('');
  const [result, setResult]         = useState(null);
  const [checkResult, setCheckResult] = useState(null);
  const [hint, setHint]             = useState(null);
  const [loading, setLoading]       = useState(false);
  const [hintLoading, setHintLoading] = useState(false);
  const [completed, setCompleted]   = useState(() => {
    try { return JSON.parse(localStorage.getItem('pg_completed') || '{}'); } catch { return {}; }
  });
  const [totalXP, setTotalXP]       = useState(() => {
    try { return parseInt(localStorage.getItem('pg_xp') || '0'); } catch { return 0; }
  });
  const [view, setView]             = useState('tasks'); // 'tasks' | 'editor' | 'schema'
  const [runError, setRunError]     = useState('');
  const textareaRef = useRef(null);

  useEffect(() => {
    axios.get(`${API}/tasks`).then(r => setTasks(r.data.tasks || []));
    axios.get(`${API}/schema`).then(r => setSchema(r.data.schema || {}));
  }, []);

  const openTask = (task) => {
    setActiveTask(task);
    setSql('-- Write your SQL here\n');
    setResult(null);
    setCheckResult(null);
    setHint(null);
    setRunError('');
    setView('editor');
  };

  const runSQL = async () => {
    if (!sql.trim() || sql.trim().startsWith('--')) return;
    setLoading(true); setResult(null); setRunError('');
    try {
      const r = await axios.post(`${API}/run`, { sql });
      setResult(r.data);
    } catch (e) {
      setRunError(e.response?.data?.detail || e.message);
    } finally {
      setLoading(false);
    }
  };

  const checkAnswer = async () => {
    if (!activeTask || !sql.trim()) return;
    setLoading(true); setCheckResult(null);
    try {
      const r = await axios.post(`${API}/check`, { task_id: activeTask.id, sql });
      setCheckResult(r.data);
      if (r.data.correct && !completed[activeTask.id]) {
        const newCompleted = { ...completed, [activeTask.id]: true };
        const newXP = totalXP + r.data.xp_earned;
        setCompleted(newCompleted);
        setTotalXP(newXP);
        localStorage.setItem('pg_completed', JSON.stringify(newCompleted));
        localStorage.setItem('pg_xp', String(newXP));
      }
    } catch (e) {
      setCheckResult({ correct: false, message: e.response?.data?.detail || e.message });
    } finally {
      setLoading(false);
    }
  };

  const getHint = async () => {
    if (!activeTask) return;
    setHintLoading(true); setHint(null);
    try {
      const r = await axios.post(`${API}/hint`, { task_id: activeTask.id, sql: sql.trim() });
      setHint(r.data);
    } catch (e) {
      setHint({ hint: activeTask.hint || 'No hint available.', tier: 'static' });
    } finally {
      setHintLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runSQL(); }
    if (e.key === 'Tab') {
      e.preventDefault();
      const ta = textareaRef.current;
      const s = ta.selectionStart, end = ta.selectionEnd;
      const next = sql.substring(0, s) + '  ' + sql.substring(end);
      setSql(next);
      requestAnimationFrame(() => { ta.selectionStart = ta.selectionEnd = s + 2; });
    }
  };

  const levelGroups = tasks.reduce((acc, t) => {
    (acc[t.level] = acc[t.level] || []).push(t);
    return acc;
  }, {});

  const completedCount = Object.keys(completed).length;
  const maxXP = tasks.reduce((s, t) => s + (t.xp || 0), 0);

  return (
    <div className="pg-root">
      {/* ── Top bar ── */}
      <div className="pg-topbar">
        <div className="pg-topbar-left">
          <Trophy size={20} className="pg-trophy" />
          <span className="pg-title">SQL Playground</span>
          <span className="pg-subtitle">Learn by doing • 13 challenges</span>
        </div>
        <div className="pg-topbar-right">
          <div className="pg-xp-bar-wrap">
            <div className="pg-xp-label">
              <Zap size={14} /> {totalXP} XP
            </div>
            <div className="pg-xp-track">
              <div className="pg-xp-fill" style={{ width: `${Math.min(100, (totalXP / maxXP) * 100)}%` }} />
            </div>
          </div>
          <div className="pg-badge">{completedCount}/{tasks.length} done</div>
          <div className="pg-view-tabs">
            <button className={`pg-vtab ${view === 'tasks' ? 'active' : ''}`} onClick={() => setView('tasks')}>
              <BookOpen size={14} /> Tasks
            </button>
            <button className={`pg-vtab ${view === 'editor' ? 'active' : ''}`} onClick={() => setView('editor')} disabled={!activeTask}>
              <TerminalSquare size={14} /> Editor
            </button>
            <button className={`pg-vtab ${view === 'schema' ? 'active' : ''}`} onClick={() => setView('schema')}>
              <Table2 size={14} /> Schema
            </button>
          </div>
        </div>
      </div>

      {/* ── Task Browser ── */}
      {view === 'tasks' && (
        <div className="pg-task-browser">
          {Object.entries(levelGroups).map(([level, lvlTasks]) => (
            <div key={level} className="pg-level-group">
              <div className="pg-level-header" style={{ borderLeftColor: LEVEL_COLORS[level] }}>
                <span className="pg-level-badge" style={{ background: LEVEL_COLORS[level] + '22', color: LEVEL_COLORS[level], borderColor: LEVEL_COLORS[level] + '55' }}>
                  Level {level}
                </span>
                <span className="pg-level-name">{LEVEL_NAMES[level]}</span>
              </div>
              <div className="pg-task-grid">
                {lvlTasks.map(task => {
                  const done = !!completed[task.id];
                  return (
                    <button
                      key={task.id}
                      className={`pg-task-card ${done ? 'done' : ''}`}
                      onClick={() => openTask(task)}
                    >
                      <div className="pg-task-card-top">
                        <span className="pg-task-cat" style={{ color: LEVEL_COLORS[level] }}>{task.category}</span>
                        <span className="pg-task-xp"><Zap size={11} /> {task.xp} XP</span>
                      </div>
                      <div className="pg-task-title">{task.title}</div>
                      <div className="pg-task-desc" dangerouslySetInnerHTML={{ __html: task.description.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>').replace(/`(.*?)`/g, '<code>$1</code>') }} />
                      <div className="pg-task-card-footer">
                        {done
                          ? <span className="pg-done-pill"><CheckCircle2 size={13} /> Completed</span>
                          : <span className="pg-start-pill"><ChevronRight size={13} /> Start</span>}
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* ── Editor ── */}
      {view === 'editor' && activeTask && (
        <div className="pg-editor-layout">
          {/* Left: task info + editor */}
          <div className="pg-editor-left">
            <div className="pg-task-info-card">
              <div className="pg-task-info-top">
                <span className="pg-task-cat" style={{ color: LEVEL_COLORS[activeTask.level] }}>
                  Level {activeTask.level} · {activeTask.category}
                </span>
                <span className="pg-task-xp"><Zap size={12} /> {activeTask.xp} XP</span>
              </div>
              <h2 className="pg-task-info-title">{activeTask.title}</h2>
              <p className="pg-task-info-desc" dangerouslySetInnerHTML={{ __html: activeTask.description.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>').replace(/`(.*?)`/g, '<code>$1</code>') }} />
            </div>

            <div className="pg-editor-wrap">
              <div className="pg-editor-header">
                <span className="pg-editor-label">SQL Editor</span>
                <span className="pg-editor-shortcut">Ctrl+Enter to run</span>
              </div>
              <textarea
                ref={textareaRef}
                className="pg-sql-editor"
                value={sql}
                onChange={e => setSql(e.target.value)}
                onKeyDown={handleKeyDown}
                spellCheck={false}
                placeholder="-- Write your SELECT query here..."
              />
              <div className="pg-editor-actions">
                <button className="pg-btn-run" onClick={runSQL} disabled={loading}>
                  {loading ? <Loader2 size={16} className="loading-spinner" /> : <Play size={16} />}
                  Run Query
                </button>
                <button className="pg-btn-check" onClick={checkAnswer} disabled={loading}>
                  <CheckCircle2 size={16} /> Submit Answer
                </button>
                <button className="pg-btn-hint" onClick={getHint} disabled={hintLoading}>
                  {hintLoading ? <Loader2 size={14} className="loading-spinner" /> : <Lightbulb size={14} />}
                  Hint
                </button>
                <button className="pg-btn-reset" onClick={() => { setSql('-- Write your SQL here\n'); setResult(null); setCheckResult(null); setHint(null); setRunError(''); }}>
                  <RotateCcw size={14} /> Reset
                </button>
              </div>
            </div>

            {/* Hint */}
            {hint && (
              <div className="pg-hint-box">
                <Lightbulb size={16} className="pg-hint-icon" />
                <div>
                  <span className="pg-hint-label">{hint.tier === 'ai' ? 'AI Hint' : 'Hint'}</span>
                  <p>{hint.hint}</p>
                </div>
              </div>
            )}

            {/* Check result */}
            {checkResult && (
              <div className={`pg-verdict ${checkResult.correct ? 'correct' : 'wrong'}`}>
                {checkResult.correct
                  ? <><CheckCircle2 size={20} /><div><strong>Correct! +{checkResult.xp_earned} XP</strong><p>{checkResult.message}</p></div></>
                  : <><XCircle size={20} /><div><strong>Not quite.</strong><p>{checkResult.message}</p></div></>}
              </div>
            )}
          </div>

          {/* Right: results */}
          <div className="pg-editor-right">
            <div className="pg-result-header">
              <span>Query Results</span>
              {result && <span className="pg-result-count">{result.row_count} row{result.row_count !== 1 ? 's' : ''}</span>}
            </div>

            {runError && (
              <div className="pg-run-error"><AlertCircle size={14} /> {runError}</div>
            )}

            {!result && !runError && (
              <div className="pg-result-empty">
                <TerminalSquare size={32} />
                <p>Run your query to see results here.</p>
              </div>
            )}

            {result && result.columns.length > 0 && (
              <div className="pg-result-table-wrap">
                <table className="pg-result-table">
                  <thead>
                    <tr>{result.columns.map(c => <th key={c}>{c}</th>)}</tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, i) => (
                      <tr key={i}>{row.map((cell, j) => <td key={j}>{cell === null ? <span className="pg-null">NULL</span> : String(cell)}</td>)}</tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Schema quick-ref */}
            <div className="pg-schema-quickref">
              <div className="pg-qr-header"><Table2 size={13} /> Available Tables</div>
              {Object.entries(schema).map(([tbl, info]) => (
                <div key={tbl} className="pg-qr-table">
                  <span className="pg-qr-tname">{tbl}</span>
                  <span className="pg-qr-cols">{info.columns.join(', ')}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* ── Schema View ── */}
      {view === 'schema' && (
        <div className="pg-schema-view">
          <div className="pg-schema-hero">
            <Table2 size={28} className="pg-trophy" />
            <div>
              <h2>Sandbox Database Schema</h2>
              <p>All challenges use this in-memory SQLite dataset. No real DB needed.</p>
            </div>
          </div>
          <div className="pg-schema-grid">
            {Object.entries(schema).map(([tbl, info]) => (
              <div key={tbl} className="pg-schema-card">
                <div className="pg-schema-card-title"><Table2 size={15} /> {tbl}</div>
                <p className="pg-schema-card-desc">{info.description}</p>
                <div className="pg-schema-cols">
                  {info.columns.map(c => <span key={c} className="chip">{c}</span>)}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── No task selected in editor ── */}
      {view === 'editor' && !activeTask && (
        <div className="pg-result-empty" style={{ flex: 1 }}>
          <Lock size={36} />
          <p>Select a task from the Tasks panel to open the editor.</p>
          <button className="pg-btn-run" style={{ marginTop: '1rem' }} onClick={() => setView('tasks')}>Browse Tasks</button>
        </div>
      )}
    </div>
  );
}
