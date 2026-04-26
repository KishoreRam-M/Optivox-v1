import React, { useState } from 'react';
import axios from 'axios';
import { 
  Database, Zap, BookOpen, BarChart3,
  TerminalSquare, Loader2, CheckCircle2, XCircle, AlertCircle,
  Unplug, Server, User, Lock, Hash, Globe
} from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

const DIALECTS = [
  { value: 'mysql',    label: 'MySQL',      defaultPort: 3306  },
  { value: 'postgres', label: 'PostgreSQL', defaultPort: 5432  },
  { value: 'oracle',   label: 'Oracle',     defaultPort: 1521  },
  { value: 'mssql',   label: 'SQL Server',  defaultPort: 1433  },
];

export default function App() {
  const [activeTab, setActiveTab]   = useState('nl_sql');
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
      await axios.post(`${API_BASE}/connect`, dbConfig);
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

        {connection && (
          <nav className="nav-menu">
            <NavBtn id="nl_sql"    icon={<TerminalSquare />} label="NL → SQL"          active={activeTab} set={setActiveTab} />
            <NavBtn id="teach"     icon={<BookOpen />}       label="Database Tutor"    active={activeTab} set={setActiveTab} />
            <NavBtn id="optimize"  icon={<Zap />}            label="Query Optimizer"   active={activeTab} set={setActiveTab} />
            <NavBtn id="schema"    icon={<BarChart3 />}      label="Schema Analysis"   active={activeTab} set={setActiveTab} />
          </nav>
        )}

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
        {!connection ? (
          <ConnectScreen
            dbConfig={dbConfig}
            setField={setField}
            handleDialectChange={handleDialectChange}
            handleConnect={handleConnect}
            connecting={connecting}
            connError={connError}
          />
        ) : (
          <>
            {activeTab === 'nl_sql'   && <NLSqlSection   connection={connection} />}
            {activeTab === 'teach'    && <TeachSection />}
            {activeTab === 'optimize' && <OptimizeSection connection={connection} />}
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
function ConnectScreen({ dbConfig, setField, handleDialectChange, handleConnect, connecting, connError }) {
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
function NLSqlSection({ connection }) {
  const [question, setQuestion] = useState('');
  const [mode,     setMode]     = useState('fast');
  const [loading,  setLoading]  = useState(false);
  const [result,   setResult]   = useState(null);
  const [error,    setError]    = useState('');

  const generate = async () => {
    if (!question) return;
    setLoading(true); setError(''); setResult(null);
    try {
      const res = await axios.post(`${API_BASE}/adia/nl-sql`, {
        question, dialect: connection.dialect, connection, mode,
      });
      setResult(res.data);
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="glass-card">
      <div className="header">
        <h1>Natural Language to SQL</h1>
        <p>Translate plain English directly into safe, validated SQL queries.</p>
      </div>

      <div className="textarea-wrapper">
        <textarea
          className="query-input"
          placeholder="e.g. Show me the top 5 users by total revenue this month..."
          value={question}
          onChange={e => setQuestion(e.target.value)}
        />
      </div>

      <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', marginBottom: '2rem' }}>
        <select value={mode} onChange={e => setMode(e.target.value)} style={{ width: '220px' }}>
          <option value="fast">Fast (Single Model)</option>
          <option value="crew">Autonomous Agents (CrewAI)</option>
        </select>
        <button className="btn-primary" onClick={generate} disabled={loading} style={{ width: '200px' }}>
          {loading ? <Loader2 className="loading-spinner" /> : 'Generate SQL'}
        </button>
      </div>

      {error && (
        <div className="status-badge error" style={{ marginBottom: '1rem' }}>
          <AlertCircle size={16} /> {error}
        </div>
      )}

      {result && (
        <div>
          <div className={`status-badge ${result.approved === false || result.safe === false ? 'error' : ''}`}>
            {result.approved !== false && result.safe !== false
              ? <CheckCircle2 size={16} />
              : <XCircle size={16} />}
            {result.approved !== false && result.safe !== false
              ? 'Validated & Safe'
              : `Rejected: ${result.safety_reason || result.rejection_reason}`}
          </div>
          <div className="code-block">{result.sql}</div>
        </div>
      )}
    </div>
  );
}

/* ────────────────────────────────────────────────────────
   2. Teaching Section
──────────────────────────────────────────────────────── */
function TeachSection() {
  const [question, setQuestion] = useState('');
  const [loading,  setLoading]  = useState(false);
  const [history,  setHistory]  = useState([]);

  const ask = async () => {
    if (!question) return;
    const q = question;
    setHistory(prev => [...prev, { role: 'user', content: q }]);
    setQuestion('');
    setLoading(true);
    try {
      const res = await axios.post(`${API_BASE}/adia/teach`, { question: q });
      setHistory(prev => [...prev, { role: 'assistant', content: res.data.answer }]);
    } catch (err) {
      setHistory(prev => [...prev, { role: 'assistant', content: 'Error: ' + (err.response?.data?.detail || err.message) }]);
    } finally {
      setLoading(false);
    }
  };

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
            whiteSpace:   'pre-wrap',
          }}>
            {h.content}
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
