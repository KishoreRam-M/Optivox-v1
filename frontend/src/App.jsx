import React, { useState, useCallback } from 'react';
import axios from 'axios';
import { 
  Database, Zap, BookOpen, BarChart3,
  TerminalSquare, Loader2, CheckCircle2, XCircle, AlertCircle,
  Unplug, Server, User, Lock, Hash, Globe, Gamepad2, Link, Play, Table2
} from 'lucide-react';
import PlaygroundSection from './PlaygroundSection';
import MarkdownRenderer from './MarkdownRenderer';
import useSession from './useSession';

const API_BASE = 'http://localhost:8000/api';

const DIALECTS = [
  { value: 'mysql',    label: 'MySQL',      defaultPort: 3306  },
  { value: 'postgres', label: 'PostgreSQL', defaultPort: 5432  },
  { value: 'oracle',   label: 'Oracle',     defaultPort: 1521  },
  { value: 'mssql',   label: 'SQL Server',  defaultPort: 1433  },
];

export default function App() {
  const sessionId = useSession();
  const [activeTab, setActiveTab]   = useState('playground');
  const [connection, setConnection] = useState(null);
  const [connecting, setConnecting] = useState(false);
  const [connError, setConnError]   = useState('');

  const [dbConfig, setDbConfig] = useState({
    host:     'localhost',
    port:     3306,
    user:     'root',
    password: '',
    database: '',
    dialect:  'mysql',
  });

  const setField = (key, val) => setDbConfig(p => ({ ...p, [key]: val }));

  const handleDialectChange = (dialect) => {
    const d = DIALECTS.find(x => x.value === dialect);
    setDbConfig(p => ({ ...p, dialect, port: d?.defaultPort ?? p.port }));
  };

  const handleConnect = async (e) => {
    e.preventDefault();
    setConnecting(true);
    setConnError('');
    try {
      await axios.post(`${API_BASE}/connect`, { ...dbConfig, session_id: sessionId });
      setConnection(dbConfig);
    } catch (err) {
      setConnError(err.response?.data?.detail || err.message);
    } finally {
      setConnecting(false);
    }
  };

  const handleDisconnect = () => {
    setConnection(null);
    setConnError('');
  };

  return (
    <div className="app-container">
      {/* ── Sidebar ── */}
      <aside className="sidebar">
        <div className="logo-container">
          <Database size={28} className="logo-icon" />
          <div className="logo-text">OptiVox DB</div>
        </div>

        <nav className="nav-menu">
          {!connection && (
            <NavBtn id="connect" icon={<Database />} label="Connect DB" active={(activeTab === 'playground' || activeTab === 'developer') ? '' : 'connect'} set={(id) => setActiveTab(id === 'connect' ? 'nl_sql' : id)} />
          )}
          {connection && (
            <>
              <NavBtn id="nl_sql"    icon={<TerminalSquare />} label="NL → SQL"          active={activeTab} set={setActiveTab} />
              <NavBtn id="teach"     icon={<BookOpen />}       label="Database Tutor"    active={activeTab} set={setActiveTab} />
              <NavBtn id="optimize"  icon={<Zap />}            label="Query Optimizer"   active={activeTab} set={setActiveTab} />
              <NavBtn id="schema"    icon={<BarChart3 />}      label="Schema Analysis"   active={activeTab} set={setActiveTab} />
            </>
          )}
          <div style={{ margin: '0.5rem 0', borderTop: '1px solid var(--border-color)' }}></div>
          <NavBtn id="playground" icon={<Gamepad2 />}      label="SQL Playground"    active={activeTab} set={setActiveTab} />
          <NavBtn id="developer"  icon={<User />}          label="Developer"         active={activeTab} set={setActiveTab} />
        </nav>

        <div className="sidebar-spacer" />

        {/* Connection status pill at bottom of sidebar */}
        {connection ? (
          <div className="conn-status-block">
            <div className="conn-status-pill connected">
              <CheckCircle2 size={14} />
              <span>{connection.dialect.toUpperCase()} · {connection.database}</span>
            </div>
            <button className="btn-disconnect" onClick={handleDisconnect}>
              <Unplug size={14} /> Disconnect
            </button>
          </div>
        ) : (
          <div className="conn-status-pill disconnected">
            <XCircle size={14} />
            <span>Not connected</span>
          </div>
        )}
      </aside>

      {/* ── Main Content ── */}
      <main className="main-content">
        {activeTab === 'playground' ? (
          <PlaygroundSection />
        ) : activeTab === 'developer' ? (
          <DeveloperSection />
        ) : !connection ? (
          <ConnectScreen
            dbConfig={dbConfig}
            setField={setField}
            setDbConfig={setDbConfig}
            handleDialectChange={handleDialectChange}
            handleConnect={handleConnect}
            connecting={connecting}
            connError={connError}
          />
        ) : (
          <>
            {activeTab === 'nl_sql'   && <NLSqlSection   connection={connection} sessionId={sessionId} />}
            {activeTab === 'teach'    && <TeachSection sessionId={sessionId} />}
            {activeTab === 'optimize' && <OptimizeSection connection={connection} sessionId={sessionId} />}
            {activeTab === 'schema'   && <SchemaSection  connection={connection} />}
          </>
        )}
      </main>
    </div>
  );
}

/* ── Nav Button ── */
function NavBtn({ id, icon, label, active, set }) {
  return (
    <button
      className={`nav-item ${active === id ? 'active' : ''}`}
      onClick={() => set(id)}
    >
      <span className="nav-icon">{icon}</span>
      {label}
    </button>
  );
}

/* ────────────────────────────────────────────────────────
   Connection Setup Screen (main area)
──────────────────────────────────────────────────────── */
function ConnectScreen({ dbConfig, setField, setDbConfig, handleDialectChange, handleConnect, connecting, connError }) {
  const [dbUrl, setDbUrl] = useState('');

  const handleUrlParse = (url) => {
    setDbUrl(url);
    if (!url.trim()) return;
    try {
      const parsed = new URL(url.trim());
      
      let dialect = parsed.protocol.replace(':', '');
      if (dialect === 'postgresql') dialect = 'postgres';
      
      const newConfig = { ...dbConfig };
      
      if (dialect) {
        const matchingDialect = DIALECTS.find(d => d.value === dialect);
        if (matchingDialect) {
          newConfig.dialect = dialect;
          newConfig.port = matchingDialect.defaultPort;
        }
      }
      
      if (parsed.hostname) newConfig.host = parsed.hostname;
      if (parsed.port) newConfig.port = parseInt(parsed.port, 10);
      if (parsed.username) newConfig.user = decodeURIComponent(parsed.username);
      if (parsed.password) newConfig.password = decodeURIComponent(parsed.password);
      if (parsed.pathname && parsed.pathname.length > 1) {
        newConfig.database = parsed.pathname.substring(1);
      }
      
      setDbConfig(newConfig);
    } catch (e) {
      // Ignore parse errors
    }
  };

  return (
    <div className="connect-screen">
      <div className="connect-hero">
        <div className="connect-hero-icon">
          <Database size={48} />
        </div>
        <h1>Connect to your Database</h1>
        <p>Enter your database credentials to start using the Agentic AI SQL Studio.</p>
      </div>

      <form className="connect-form glass-card" onSubmit={handleConnect}>
        {/* Paste URL */}
        <div className="connect-field-group" style={{ marginBottom: '0.5rem' }}>
          <label><Link size={14} /> Paste Connection URL</label>
          <input
            type="text"
            placeholder="mysql://user:pass@host:3306/db"
            value={dbUrl}
            onChange={e => handleUrlParse(e.target.value)}
            style={{ borderColor: 'var(--accent-primary)' }}
          />
        </div>
        
        <div style={{ display: 'flex', alignItems: 'center', margin: '0.5rem 0' }}>
          <div style={{ flex: 1, height: '1px', background: 'var(--border-color)' }}></div>
          <span style={{ padding: '0 1rem', fontSize: '0.8rem', color: 'var(--text-secondary)' }}>OR ENTER MANUALLY</span>
          <div style={{ flex: 1, height: '1px', background: 'var(--border-color)' }}></div>
        </div>

        {/* Dialect Selector */}
        <div className="connect-dialect-row">
          {DIALECTS.map(d => (
            <button
              key={d.value}
              type="button"
              className={`dialect-chip ${dbConfig.dialect === d.value ? 'selected' : ''}`}
              onClick={() => handleDialectChange(d.value)}
            >
              <Database size={16} />
              {d.label}
            </button>
          ))}
        </div>

        <div className="connect-fields">
          {/* Host + Port */}
          <div className="connect-row">
            <div className="connect-field-group flex-3">
              <label><Globe size={14} /> Host</label>
              <input
                type="text"
                placeholder="localhost or IP address"
                value={dbConfig.host}
                onChange={e => setField('host', e.target.value)}
                required
              />
            </div>
            <div className="connect-field-group flex-1">
              <label><Hash size={14} /> Port</label>
              <input
                type="number"
                placeholder="3306"
                value={dbConfig.port}
                onChange={e => setField('port', Number(e.target.value))}
                required
              />
            </div>
          </div>

          {/* Database Name */}
          <div className="connect-field-group">
            <label><Server size={14} /> Database Name</label>
            <input
              type="text"
              placeholder="e.g. my_database"
              value={dbConfig.database}
              onChange={e => setField('database', e.target.value)}
              required
            />
          </div>

          {/* User + Password */}
          <div className="connect-row">
            <div className="connect-field-group flex-1">
              <label><User size={14} /> Username</label>
              <input
                type="text"
                placeholder="root"
                value={dbConfig.user}
                onChange={e => setField('user', e.target.value)}
                required
              />
            </div>
            <div className="connect-field-group flex-1">
              <label><Lock size={14} /> Password</label>
              <input
                type="password"
                placeholder="••••••••"
                value={dbConfig.password}
                onChange={e => setField('password', e.target.value)}
              />
            </div>
          </div>
        </div>

        {connError && (
          <div className="conn-error-box">
            <AlertCircle size={16} />
            <span>{connError}</span>
          </div>
        )}

        <button type="submit" className="btn-primary btn-connect-submit" disabled={connecting}>
          {connecting
            ? <><Loader2 className="loading-spinner" size={18} /> Connecting…</>
            : <><Database size={18} /> Connect to Database</>
          }
        </button>
      </form>
    </div>
  );
}

/* ────────────────────────────────────────────────────────
   1. Natural Language SQL Generation
──────────────────────────────────────────────────────── */
function NLSqlSection({ connection, sessionId }) {
  const [question, setQuestion] = useState('');
  const [mode,     setMode]     = useState('fast');
  const [loading,  setLoading]  = useState(false);
  const [result,   setResult]   = useState(null);
  const [error,    setError]    = useState('');

  const [explainLoading, setExplainLoading] = useState(false);
  const [explanation,    setExplanation]    = useState('');

  const [execLoading, setExecLoading] = useState(false);
  const [execResult,  setExecResult]  = useState(null);
  const [execError,   setExecError]   = useState('');

  const generate = useCallback(async () => {
    if (!question.trim()) return;
    setLoading(true); setError(''); setResult(null);
    setExplanation(''); setExecResult(null); setExecError('');
    try {
      const res = await axios.post(`${API_BASE}/adia/nl-sql`, {
        question, dialect: connection.dialect, connection, mode, session_id: sessionId,
      });
      setResult(res.data);
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  }, [question, mode, connection, sessionId]);

  const explainQuery = useCallback(async () => {
    if (!result?.sql) return;
    setExplainLoading(true); setExplanation('');
    try {
      const res = await axios.post(`${API_BASE}/adia/teach`, {
        question: `Explain the following SQL query step by step, clearly and concisely. Use markdown formatting with numbered steps:\n\n\`\`\`sql\n${result.sql}\n\`\`\``,
        session_id: sessionId,
      });
      setExplanation(res.data.answer);
    } catch (err) {
      setExplanation('**Error:** ' + (err.response?.data?.detail || err.message));
    } finally {
      setExplainLoading(false);
    }
  }, [result, sessionId]);

  const executeQuery = useCallback(async () => {
    if (!result?.sql) return;
    setExecLoading(true); setExecResult(null); setExecError('');
    try {
      const res = await axios.post(`${API_BASE}/execute`, {
        sql: result.sql,
        connection,
        session_id: sessionId,
      });
      setExecResult(res.data);
    } catch (err) {
      setExecError(err.response?.data?.detail || err.message);
    } finally {
      setExecLoading(false);
    }
  }, [result, connection, sessionId]);

  const isSafe = result && result.approved !== false && result.safe !== false;

  return (
    <div className="glass-card">
      <div className="header">
        <h1>Natural Language to SQL</h1>
        <p>Translate plain English directly into safe, validated SQL queries — then run and explain them inline.</p>
      </div>

      <div className="textarea-wrapper">
        <textarea
          className="query-input"
          placeholder="e.g. Show me the top 5 students enrolled in 2024..."
          value={question}
          onChange={e => setQuestion(e.target.value)}
          onKeyDown={e => { if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') generate(); }}
        />
      </div>

      <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', marginBottom: '1.5rem' }}>
        <select value={mode} onChange={e => setMode(e.target.value)} style={{ width: '220px' }}>
          <option value="fast">Fast (Single Model)</option>
          <option value="crew">Autonomous Agents (CrewAI)</option>
        </select>
        <button className="btn-primary" onClick={generate} disabled={loading} style={{ width: '200px' }}>
          {loading ? <><Loader2 className="loading-spinner" size={18} /> Generating…</> : 'Generate SQL'}
        </button>
        <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>Ctrl+Enter to generate</span>
      </div>

      {error && (
        <div className="status-badge error" style={{ marginBottom: '1rem' }}>
          <AlertCircle size={16} /> {error}
        </div>
      )}

      {result && (
        <div>
          {/* Safety badge */}
          <div className={`status-badge ${isSafe ? '' : 'error'}`} style={{ marginBottom: '0.75rem' }}>
            {isSafe ? <CheckCircle2 size={16} /> : <XCircle size={16} />}
            {isSafe ? 'Validated & Safe' : `Rejected: ${result.safety_reason || result.rejection_reason}`}
          </div>

          {/* Generated SQL */}
          <div className="code-block">{result.sql}</div>

          {/* Action buttons */}
          {isSafe && (
            <div className="nl-action-row">
              <button className="nl-btn-run" onClick={executeQuery} disabled={execLoading}>
                {execLoading ? <Loader2 className="loading-spinner" size={15} /> : <Play size={15} />}
                {execLoading ? 'Running…' : 'Run Query'}
              </button>
              <button className="nl-btn-explain" onClick={explainQuery} disabled={explainLoading}>
                {explainLoading ? <Loader2 className="loading-spinner" size={15} /> : <BookOpen size={15} />}
                {explainLoading ? 'Explaining…' : 'Explain Query'}
              </button>
            </div>
          )}

          {/* Explanation panel */}
          {(explanation || explainLoading) && (
            <div className="explain-panel">
              <div className="explain-panel-header">
                <BookOpen size={16} /> Step-by-Step Explanation
              </div>
              {explainLoading
                ? <div style={{ display:'flex', alignItems:'center', gap:'0.5rem', color:'var(--text-secondary)' }}><Loader2 className="loading-spinner" size={16}/> Generating explanation…</div>
                : <MarkdownRenderer>{explanation}</MarkdownRenderer>
              }
            </div>
          )}

          {/* Execution error */}
          {execError && (
            <div className="status-badge error" style={{ margin: '1rem 0' }}>
              <AlertCircle size={16} /> {execError}
            </div>
          )}

          {/* Execution results */}
          {execResult && (
            <div className="exec-result-wrap">
              <div className="exec-result-header">
                <strong><Table2 size={16} /> Execution Results</strong>
                <div className="exec-result-meta">
                  {execResult.statements_executed > 1 && (
                    <span>{execResult.statements_executed} statements</span>
                  )}
                  {execResult.row_count !== undefined && (
                    <span className="exec-badge">{execResult.row_count} rows</span>
                  )}
                  {execResult.rows_affected !== undefined && (
                    <span className="exec-badge">{execResult.rows_affected} rows affected</span>
                  )}
                  <span>{execResult.duration_ms} ms</span>
                </div>
              </div>

              {execResult.columns && execResult.columns.length > 0 ? (
                <>
                  <div className="exec-table-scroll">
                    <table className="exec-table">
                      <thead>
                        <tr>{execResult.columns.map(c => <th key={c}>{c}</th>)}</tr>
                      </thead>
                      <tbody>
                        {execResult.rows.map((row, i) => (
                          <tr key={i}>
                            {row.map((cell, j) => (
                              <td key={j}>
                                {cell === null
                                  ? <span className="null-cell">NULL</span>
                                  : String(cell)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="exec-footer">{execResult.row_count} row{execResult.row_count !== 1 ? 's' : ''} returned.</div>
                </>
              ) : (
                <div style={{ padding: '1.5rem', textAlign: 'center', color: 'var(--text-secondary)' }}>
                  ✅ {execResult.statements_executed} statement{execResult.statements_executed !== 1 ? 's' : ''} executed successfully.
                  {execResult.rows_affected !== undefined && execResult.rows_affected > 0 && ` ${execResult.rows_affected} rows affected.`}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* ────────────────────────────────────────────────────────
   2. Teaching Section
──────────────────────────────────────────────────────── */
function TeachSection({ sessionId }) {
  const [question, setQuestion] = useState('');
  const [loading,  setLoading]  = useState(false);
  const [history,  setHistory]  = useState([]);

  const ask = useCallback(async () => {
    if (!question.trim()) return;
    const q = question.trim();
    setHistory(prev => [...prev, { role: 'user', content: q }]);
    setQuestion('');
    setLoading(true);
    try {
      const res = await axios.post(`${API_BASE}/adia/teach`, { question: q, session_id: sessionId });
      setHistory(prev => [...prev, { role: 'assistant', content: res.data.answer }]);
    } catch (err) {
      setHistory(prev => [...prev, { role: 'assistant', content: '**Error:** ' + (err.response?.data?.detail || err.message) }]);
    } finally {
      setLoading(false);
    }
  }, [question, sessionId]);

  return (
    <div className="glass-card" style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 6rem)' }}>
      <div className="header" style={{ marginBottom: '1rem' }}>
        <h1>Database Tutor</h1>
        <p>Learn database concepts and SQL with interactive examples.</p>
      </div>

      <div style={{ flex: 1, overflowY: 'auto', marginBottom: '1rem', display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        {history.map((h, i) => (
          <div key={i} style={{
            background:   h.role === 'user' ? 'rgba(138,43,226,0.1)' : 'var(--bg-surface-elevated)',
            padding:      '1.25rem',
            borderRadius: 'var(--radius-md)',
            border:       h.role === 'user' ? '1px solid rgba(138,43,226,0.3)' : '1px solid var(--border-color)',
            alignSelf:    h.role === 'user' ? 'flex-end' : 'flex-start',
            maxWidth:     '80%',
          }}>
            {h.role === 'user'
              ? <span style={{ color: 'var(--text-primary)' }}>{h.content}</span>
              : <MarkdownRenderer>{h.content}</MarkdownRenderer>
            }
          </div>
        ))}
        {loading && (
          <div style={{ alignSelf: 'flex-start', color: 'var(--text-secondary)' }}>
            <Loader2 className="loading-spinner" size={20} />
          </div>
        )}
      </div>

      <div style={{ display: 'flex', gap: '1rem' }}>
        <input
          style={{ flex: 1 }}
          placeholder="e.g. Explain how LEFT JOIN works with an example..."
          value={question}
          onChange={e => setQuestion(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && ask()}
        />
        <button className="btn-primary" style={{ width: '150px' }} onClick={ask} disabled={loading}>
          Send
        </button>
      </div>
    </div>
  );
}

/* ────────────────────────────────────────────────────────
   3. Query Optimization
──────────────────────────────────────────────────────── */
function OptimizeSection({ connection }) {
  const [sql,     setSql]     = useState('');
  const [explain, setExplain] = useState('');
  const [loading, setLoading] = useState(false);
  const [result,  setResult]  = useState(null);

  const optimize = async () => {
    if (!sql) return;
    setLoading(true); setResult(null);
    try {
      const res = await axios.post(`${API_BASE}/adia/optimize`, {
        sql, dialect: connection.dialect, explain_output: explain,
      });
      setResult(res.data);
    } catch (err) {
      alert('Error: ' + (err.response?.data?.detail || err.message));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="glass-card">
      <div className="header">
        <h1>Query Optimizer</h1>
        <p>Identify bottlenecks and get automatic rewrite suggestions.</p>
      </div>

      <div className="grid-cols-2">
        <div>
          <label style={{ display: 'block', marginBottom: '0.5rem', color: 'var(--text-secondary)' }}>Raw SQL</label>
          <textarea className="query-input" value={sql} onChange={e => setSql(e.target.value)} />
        </div>
        <div>
          <label style={{ display: 'block', marginBottom: '0.5rem', color: 'var(--text-secondary)' }}>EXPLAIN Plan (Optional)</label>
          <textarea className="query-input" value={explain} onChange={e => setExplain(e.target.value)} />
        </div>
      </div>

      <button className="btn-primary" onClick={optimize} disabled={loading} style={{ width: '250px', marginTop: '1rem' }}>
        {loading ? <Loader2 className="loading-spinner" /> : 'Analyze Performance'}
      </button>

      {result && (
        <div style={{ marginTop: '2rem' }}>
          {result.rewritten_sql && (
            <>
              <h3 style={{ color: 'var(--success)' }}>Optimized SQL</h3>
              <div className="code-block">{result.rewritten_sql}</div>
            </>
          )}
          <div className="grid-cols-2" style={{ marginTop: '1.5rem' }}>
            <div style={{ background: 'var(--bg-surface-elevated)', padding: '1.5rem', borderRadius: 'var(--radius-md)' }}>
              <h3 style={{ marginBottom: '1rem', color: 'var(--accent-primary)' }}>Performance Tips</h3>
              <ul style={{ paddingLeft: '1.2rem' }}>
                {(result.tips || []).map((t, i) => <li key={i} style={{ marginBottom: '0.5rem' }}>{t}</li>)}
              </ul>
            </div>
            <div style={{ background: 'var(--bg-surface-elevated)', padding: '1.5rem', borderRadius: 'var(--radius-md)' }}>
              <h3 style={{ marginBottom: '1rem', color: 'var(--error)' }}>Identified Issues</h3>
              <ul style={{ paddingLeft: '1.2rem' }}>
                {(result.issues || []).map((t, i) => (
                  <li key={i} style={{ marginBottom: '0.5rem' }}>
                    <strong>{t.type}</strong>: {t.description}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ────────────────────────────────────────────────────────
   4. Schema Analysis
──────────────────────────────────────────────────────── */
function SchemaSection({ connection }) {
  const [loading, setLoading] = useState(false);
  const [result,  setResult]  = useState(null);

  const analyze = async () => {
    setLoading(true); setResult(null);
    try {
      const res = await axios.post(`${API_BASE}/adia/schema-analysis`, connection);
      setResult(res.data);
    } catch (err) {
      alert('Error: ' + (err.response?.data?.detail || err.message));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="glass-card">
      <div className="header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <h1>Schema Analysis & Optimization</h1>
          <p>Holistic view of your database structure with AI-driven insights.</p>
        </div>
        <button className="btn-primary" onClick={analyze} disabled={loading} style={{ width: '200px' }}>
          {loading ? <Loader2 className="loading-spinner" /> : 'Run Analysis'}
        </button>
      </div>

      {result && (
        <div style={{ marginTop: '2rem' }}>
          <div style={{ display: 'flex', gap: '1rem', marginBottom: '2rem' }}>
            <div className="status-badge"><Database size={16} /> {result.table_count} Tables Detected</div>
            {result.isolated_tables?.length > 0 && (
              <div className="status-badge error"><AlertCircle size={16} /> {result.isolated_tables.length} Isolated Tables</div>
            )}
            {result.no_index_tables?.length > 0 && (
              <div className="status-badge error"><AlertCircle size={16} /> {result.no_index_tables.length} Tables without PK</div>
            )}
          </div>

          <div className="grid-cols-2">
            <div style={{ background: 'var(--bg-surface-elevated)', padding: '1.5rem', borderRadius: 'var(--radius-md)' }}>
              <h3 style={{ marginBottom: '1rem', color: 'var(--accent-primary)' }}>AI Recommendations</h3>
              <ul style={{ paddingLeft: '1.2rem' }}>
                {(result.ai_recommendations || []).map((t, i) => <li key={i} style={{ marginBottom: '0.5rem' }}>{t}</li>)}
              </ul>
            </div>
            <div style={{ background: 'var(--bg-surface-elevated)', padding: '1.5rem', borderRadius: 'var(--radius-md)' }}>
              <h3 style={{ marginBottom: '1rem', color: 'var(--success)' }}>General Performance Tips</h3>
              <ul style={{ paddingLeft: '1.2rem' }}>
                {(result.performance_tips || []).map((t, i) => <li key={i} style={{ marginBottom: '0.5rem' }}>{t}</li>)}
              </ul>
            </div>
          </div>

          {result.missing_index_suggestions?.length > 0 && (
            <div style={{ marginTop: '1.5rem', background: 'var(--bg-surface-elevated)', padding: '1.5rem', borderRadius: 'var(--radius-md)' }}>
              <h3 style={{ marginBottom: '1rem', color: 'var(--error)' }}>Missing Index Suggestions</h3>
              <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left' }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid var(--border-color)' }}>
                    <th style={{ padding: '0.5rem' }}>Table</th>
                    <th style={{ padding: '0.5rem' }}>Column</th>
                    <th style={{ padding: '0.5rem' }}>Reason</th>
                  </tr>
                </thead>
                <tbody>
                  {result.missing_index_suggestions.map((s, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid var(--border-color)' }}>
                      <td style={{ padding: '0.5rem' }}>{s.table}</td>
                      <td style={{ padding: '0.5rem' }}>{s.column}</td>
                      <td style={{ padding: '0.5rem', color: 'var(--text-secondary)' }}>{s.reason}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* ────────────────────────────────────────────────────────
   5. Developer Section
──────────────────────────────────────────────────────── */
function DeveloperSection() {
  return (
    <div className="glass-card" style={{ maxWidth: '800px', margin: '0 auto', padding: '2rem', height: 'fit-content' }}>
      <div className="header" style={{ marginBottom: '2rem' }}>
        <h1>🚀 Developer</h1>
      </div>
      
      <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
        <div>
          <h2 style={{ fontSize: '1.8rem', color: 'var(--accent-primary)', marginBottom: '0.5rem' }}>Kishore Ram M</h2>
          <div style={{ display: 'flex', gap: '1.5rem', marginBottom: '1rem' }}>
            <a href="https://github.com/KishoreRam-M" target="_blank" rel="noopener noreferrer" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', color: 'var(--text-primary)', textDecoration: 'none', fontWeight: '500' }}>
              🔗 GitHub
            </a>
            <a href="https://www.linkedin.com/in/kishoreramm/" target="_blank" rel="noopener noreferrer" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', color: 'var(--text-primary)', textDecoration: 'none', fontWeight: '500' }}>
              🔗 LinkedIn
            </a>
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem', lineHeight: '1.6', color: 'var(--text-secondary)', fontSize: '1.05rem' }}>
          <p>
            I am a passionate AI & Machine Learning enthusiast with a deep interest in the rapidly evolving world of Generative AI. I enjoy building intelligent systems, experimenting with new technologies, and continuously expanding my knowledge to stay at the forefront of innovation.
          </p>
          <p>
            Currently, I am a final-year Computer Science and Engineering student at Vel Tech Multi Tech Engineering College. My academic journey has equipped me with a strong foundation in software development, data structures, algorithms, and system design, enabling me to approach complex problems with a structured and analytical mindset.
          </p>
          <p>
            I am also an aspiring Data Engineer, driven by a keen interest in designing scalable data pipelines, working with large datasets, and transforming raw data into meaningful insights. I am eager to contribute to real-world projects where I can apply my skills in data engineering, machine learning, and AI-driven solutions.
          </p>
        </div>
      </div>
    </div>
  );
}
