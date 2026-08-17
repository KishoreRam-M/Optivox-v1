import React, { useState, useEffect, useCallback, useRef } from 'react';
import axios from 'axios';
import {
  Upload, Database, Table2, Play, Trash2, Eye, ChevronRight,
  CheckCircle2, AlertCircle, Loader2, FileText, Building2,
  Sparkles, TerminalSquare, RefreshCw, X, ArrowLeft, BarChart3,
  Hash, Search, Download, Info
} from 'lucide-react';

const API = (import.meta.env.VITE_API_BASE_URL || '/api') + '/csvdb';

// ── Utility ────────────────────────────────────────────────────────────────

const fmtRows = (n) => n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(n);

const TYPE_COLORS = {
  INTEGER: '#0ea5e9',
  REAL:    '#f59e0b',
  TEXT:    '#10b981',
};

// ── Sub-components ─────────────────────────────────────────────────────────

function TypeBadge({ type }) {
  const color = TYPE_COLORS[type] || '#a0a0a0';
  return (
    <span style={{
      fontSize: '0.68rem', fontFamily: 'var(--font-mono)', fontWeight: 600,
      background: color + '22', color, border: `1px solid ${color}44`,
      borderRadius: '4px', padding: '1px 6px', flexShrink: 0
    }}>{type}</span>
  );
}

function ResultTable({ columns, rows }) {
  if (!columns || columns.length === 0) return null;
  return (
    <div className="csvdb-result-wrap">
      <table className="csvdb-result-table">
        <thead>
          <tr>{columns.map(c => <th key={c}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {row.map((cell, j) => (
                <td key={j}>
                  {cell === null ? <span className="pg-null">NULL</span> : String(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Upload Step ────────────────────────────────────────────────────────────

function UploadStep({ onCreated }) {
  const [companyName, setCompanyName]   = useState('');
  const [description, setDescription]  = useState('');
  const [files, setFiles]               = useState([]);
  const [uploading, setUploading]       = useState(false);
  const [progress, setProgress]         = useState(0);
  const [error, setError]               = useState('');
  const [dragOver, setDragOver]         = useState(false);
  const fileRef = useRef();

  const addFiles = (incoming) => {
    const csvs = Array.from(incoming).filter(f =>
      f.name.toLowerCase().endsWith('.csv') || f.type === 'text/csv'
    );
    if (csvs.length === 0) { setError('Only CSV files are accepted.'); return; }
    setFiles(prev => {
      const names = new Set(prev.map(f => f.name));
      return [...prev, ...csvs.filter(f => !names.has(f.name))];
    });
    setError('');
  };

  const removeFile = (name) => setFiles(prev => prev.filter(f => f.name !== name));

  const handleDrop = (e) => {
    e.preventDefault(); setDragOver(false);
    addFiles(e.dataTransfer.files);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!companyName.trim()) { setError('Company name is required.'); return; }
    if (files.length === 0) { setError('Upload at least one CSV file.'); return; }

    setUploading(true); setError(''); setProgress(10);
    const fd = new FormData();
    fd.append('company_name', companyName.trim());
    fd.append('description', description.trim());
    files.forEach(f => fd.append('files', f));

    // Simulate progress ticks
    const ticker = setInterval(() => setProgress(p => Math.min(p + 8, 85)), 400);
    try {
      const res = await axios.post(`${API}/upload`, fd, {
        headers: { 'Content-Type': 'multipart/form-data' }
      });
      clearInterval(ticker);
      setProgress(100);
      setTimeout(() => onCreated(res.data), 600);
    } catch (err) {
      clearInterval(ticker);
      setProgress(0);
      setError(err.response?.data?.detail || err.message);
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="csvdb-upload-root">
      {/* Hero */}
      <div className="csvdb-hero">
        <div className="csvdb-hero-icon">
          <Sparkles size={32} />
        </div>
        <div>
          <h1 className="csvdb-hero-title">Agentic CSV Database</h1>
          <p className="csvdb-hero-sub">
            Upload CSV files → auto-generate schema → query with SQL instantly.
            Each company gets an isolated, persistent database.
          </p>
        </div>
      </div>

      <form className="csvdb-form" onSubmit={handleSubmit}>
        {/* Company info */}
        <div className="csvdb-section-card">
          <div className="csvdb-section-title"><Building2 size={16} /> Company Details</div>
          <div className="csvdb-field-row">
            <div className="csvdb-field">
              <label>Company Name <span style={{ color: 'var(--error)' }}>*</span></label>
              <input
                type="text"
                placeholder="e.g. Acme Corp"
                value={companyName}
                onChange={e => setCompanyName(e.target.value)}
                disabled={uploading}
              />
            </div>
            <div className="csvdb-field">
              <label>Description <span style={{ color: 'var(--text-secondary)', fontWeight: 400 }}>(optional)</span></label>
              <input
                type="text"
                placeholder="e.g. Q1 2024 sales data"
                value={description}
                onChange={e => setDescription(e.target.value)}
                disabled={uploading}
              />
            </div>
          </div>
        </div>

        {/* Drop zone */}
        <div className="csvdb-section-card">
          <div className="csvdb-section-title"><Upload size={16} /> Upload CSV Files</div>
          <div
            className={`csvdb-dropzone ${dragOver ? 'active' : ''}`}
            onDragOver={e => { e.preventDefault(); setDragOver(true); }}
            onDragLeave={() => setDragOver(false)}
            onDrop={handleDrop}
            onClick={() => fileRef.current?.click()}
          >
            <input
              ref={fileRef} type="file" multiple accept=".csv,text/csv"
              style={{ display: 'none' }}
              onChange={e => addFiles(e.target.files)}
            />
            <Upload size={28} style={{ opacity: 0.5, marginBottom: '0.5rem' }} />
            <p style={{ fontWeight: 600 }}>Drop CSV files here or click to browse</p>
            <p style={{ fontSize: '0.82rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
              Each file becomes a table. Multiple files supported.
            </p>
          </div>

          {files.length > 0 && (
            <div className="csvdb-file-list">
              {files.map(f => (
                <div key={f.name} className="csvdb-file-chip">
                  <FileText size={14} style={{ flexShrink: 0 }} />
                  <span className="csvdb-file-name">{f.name}</span>
                  <span className="csvdb-file-size">{(f.size / 1024).toFixed(1)} KB</span>
                  <button type="button" className="csvdb-file-remove" onClick={() => removeFile(f.name)}>
                    <X size={13} />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {error && (
          <div className="csvdb-error">
            <AlertCircle size={15} /> {error}
          </div>
        )}

        {/* Progress */}
        {uploading && (
          <div className="csvdb-progress-wrap">
            <div className="csvdb-progress-labels">
              <span style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
                <Loader2 size={14} className="loading-spinner" /> Generating database…
              </span>
              <span>{progress}%</span>
            </div>
            <div className="csvdb-progress-track">
              <div className="csvdb-progress-fill" style={{ width: `${progress}%` }} />
            </div>
            <p style={{ fontSize: '0.78rem', color: 'var(--text-secondary)', marginTop: '0.4rem' }}>
              Inferring column types, creating schema, seeding data…
            </p>
          </div>
        )}

        <button type="submit" className="csvdb-submit-btn" disabled={uploading}>
          {uploading
            ? <><Loader2 size={17} className="loading-spinner" /> Creating…</>
            : <><Sparkles size={17} /> Generate Database</>}
        </button>
      </form>
    </div>
  );
}

// ── Created Confirmation ───────────────────────────────────────────────────

function CreatedView({ result, onExplore, onBack }) {
  return (
    <div className="csvdb-created-root">
      <div className="csvdb-created-header">
        <CheckCircle2 size={44} style={{ color: 'var(--success)' }} />
        <h2>Database Created!</h2>
        <p>{result.message}</p>
      </div>

      <div className="csvdb-created-tables">
        {result.tables.map(tbl => (
          <div key={tbl.table_name} className="csvdb-created-tbl-card">
            <div className="csvdb-created-tbl-header">
              <Table2 size={16} style={{ color: 'var(--accent-primary)' }} />
              <span className="csvdb-created-tbl-name">{tbl.table_name}</span>
              <span className="csvdb-created-tbl-meta">{fmtRows(tbl.row_count)} rows</span>
            </div>
            <div className="csvdb-created-cols">
              {tbl.columns.map(c => (
                <div key={c.name} className="csvdb-created-col-row">
                  <span className="csvdb-created-col-name">{c.name}</span>
                  <TypeBadge type={c.type} />
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>

      <div className="csvdb-created-actions">
        <button className="csvdb-btn-secondary" onClick={onBack}>
          <ArrowLeft size={15} /> Back to Databases
        </button>
        <button className="csvdb-btn-primary" onClick={() => onExplore(result.db_id)}>
          <TerminalSquare size={15} /> Open Query Explorer
        </button>
      </div>
    </div>
  );
}

// ── Database List ─────────────────────────────────────────────────────────

function DatabaseList({ onSelect, onUpload, refreshKey }) {
  const [dbs, setDbs]         = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState('');
  const [deleting, setDeleting] = useState(null);

  const loadDbs = useCallback(() => {
    setLoading(true); setError('');
    axios.get(`${API}/list`)
      .then(r => setDbs(r.data.databases || []))
      .catch(e => setError(e.response?.data?.detail || e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { loadDbs(); }, [loadDbs, refreshKey]);

  const handleDelete = async (db_id, name) => {
    if (!window.confirm(`Delete "${name}"? This cannot be undone.`)) return;
    setDeleting(db_id);
    try {
      await axios.delete(`${API}/${db_id}`);
      setDbs(prev => prev.filter(d => d.db_id !== db_id));
    } catch (e) {
      alert(e.response?.data?.detail || e.message);
    } finally {
      setDeleting(null);
    }
  };

  return (
    <div className="csvdb-list-root">
      <div className="csvdb-list-header">
        <div>
          <h2 className="csvdb-list-title">Company Databases</h2>
          <p className="csvdb-list-sub">Each company has an isolated, persistent SQLite database.</p>
        </div>
        <div style={{ display: 'flex', gap: '0.75rem' }}>
          <button className="csvdb-btn-secondary" onClick={loadDbs}>
            <RefreshCw size={14} /> Refresh
          </button>
          <button className="csvdb-btn-primary" onClick={onUpload}>
            <Upload size={14} /> New Database
          </button>
        </div>
      </div>

      {loading && (
        <div className="csvdb-list-empty">
          <Loader2 size={32} className="loading-spinner" style={{ color: 'var(--accent-primary)' }} />
          <p>Loading databases…</p>
        </div>
      )}

      {!loading && error && (
        <div className="csvdb-list-empty">
          <AlertCircle size={32} style={{ color: 'var(--error)' }} />
          <p style={{ color: 'var(--error)' }}>{error}</p>
          <button className="csvdb-btn-secondary" onClick={loadDbs} style={{ marginTop: '1rem' }}>
            <RefreshCw size={14} /> Retry
          </button>
        </div>
      )}

      {!loading && !error && dbs.length === 0 && (
        <div className="csvdb-list-empty">
          <Database size={48} style={{ opacity: 0.3 }} />
          <h3 style={{ marginTop: '1rem' }}>No databases yet</h3>
          <p style={{ color: 'var(--text-secondary)', margin: '0.5rem 0 1.5rem' }}>
            Upload CSV files to automatically generate a company database.
          </p>
          <button className="csvdb-btn-primary" onClick={onUpload}>
            <Upload size={14} /> Create First Database
          </button>
        </div>
      )}

      {!loading && !error && dbs.length > 0 && (
        <div className="csvdb-db-grid">
          {dbs.map(db => (
            <div key={db.db_id} className="csvdb-db-card">
              <div className="csvdb-db-card-top">
                <div className="csvdb-db-icon">
                  <Building2 size={20} />
                </div>
                <div className="csvdb-db-info">
                  <div className="csvdb-db-name">{db.company_name}</div>
                  {db.description && <div className="csvdb-db-desc">{db.description}</div>}
                </div>
              </div>
              <div className="csvdb-db-stats">
                <span><Table2 size={12} /> {db.table_count} table{db.table_count !== 1 ? 's' : ''}</span>
                <span><Hash size={12} /> {fmtRows(db.total_rows)} rows</span>
                <span style={{ marginLeft: 'auto', fontSize: '0.72rem', color: 'var(--text-secondary)' }}>
                  {new Date(db.created_at).toLocaleDateString()}
                </span>
              </div>
              <div className="csvdb-db-actions">
                <button className="csvdb-btn-secondary csvdb-btn-sm" onClick={() => onSelect(db.db_id, db.company_name)}>
                  <TerminalSquare size={13} /> Query
                </button>
                <button
                  className="csvdb-btn-danger csvdb-btn-sm"
                  onClick={() => handleDelete(db.db_id, db.company_name)}
                  disabled={deleting === db.db_id}
                >
                  {deleting === db.db_id ? <Loader2 size={13} className="loading-spinner" /> : <Trash2 size={13} />}
                  Delete
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Query Explorer ────────────────────────────────────────────────────────

function QueryExplorer({ dbId, companyName, onBack }) {
  const [schema, setSchema]         = useState({});
  const [schemaLoading, setSchemaLoading] = useState(true);
  const [sql, setSql]               = useState('-- Write your SQL here\nSELECT * FROM ');
  const [result, setResult]         = useState(null);
  const [running, setRunning]       = useState(false);
  const [runError, setRunError]     = useState('');
  const [activeTable, setActiveTable] = useState(null);
  const [preview, setPreview]       = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [schemaTab, setSchemaTab]   = useState('schema'); // 'schema' | 'preview'
  const textareaRef = useRef();

  useEffect(() => {
    setSchemaLoading(true);
    axios.get(`${API}/${dbId}/schema`)
      .then(r => {
        const s = r.data.schema || {};
        setSchema(s);
        const firstTable = Object.keys(s)[0];
        if (firstTable) {
          setActiveTable(firstTable);
          setSql(`-- Write your SQL here\nSELECT * FROM "${firstTable}" LIMIT 50;`);
        }
      })
      .catch(e => console.error('Schema load failed:', e))
      .finally(() => setSchemaLoading(false));
  }, [dbId]);

  const loadPreview = async (tblName) => {
    setPreviewLoading(true); setPreview(null); setSchemaTab('preview');
    try {
      const r = await axios.get(`${API}/${dbId}/preview/${tblName}`);
      setPreview({ ...r.data, table: tblName });
    } catch (e) {
      setPreview({ error: e.response?.data?.detail || e.message });
    } finally {
      setPreviewLoading(false);
    }
  };

  const runQuery = async () => {
    const cleanSql = sql.replace(/--.*$/gm, '').trim();
    if (!cleanSql) return;
    setRunning(true); setResult(null); setRunError('');
    try {
      const r = await axios.post(`${API}/${dbId}/query`, { sql });
      setResult(r.data);
    } catch (e) {
      setRunError(e.response?.data?.detail || e.message);
    } finally {
      setRunning(false);
    }
  };

  const handleKeyDown = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runQuery(); }
    if (e.key === 'Tab') {
      e.preventDefault();
      const ta = textareaRef.current;
      const s = ta.selectionStart, end = ta.selectionEnd;
      setSql(sql.substring(0, s) + '  ' + sql.substring(end));
      requestAnimationFrame(() => { ta.selectionStart = ta.selectionEnd = s + 2; });
    }
  };

  const quickSelect = (tbl) => {
    setActiveTable(tbl);
    setSql(`SELECT * FROM "${tbl}" LIMIT 50;`);
    textareaRef.current?.focus();
  };

  return (
    <div className="csvdb-explorer-root">
      {/* Top bar */}
      <div className="csvdb-explorer-topbar">
        <button className="csvdb-btn-secondary csvdb-btn-sm" onClick={onBack}>
          <ArrowLeft size={13} /> All Databases
        </button>
        <div className="csvdb-explorer-title">
          <Building2 size={16} style={{ color: 'var(--accent-primary)' }} />
          <span>{companyName}</span>
          <span className="csvdb-explorer-dbid">#{dbId}</span>
        </div>
        <div style={{ flex: 1 }} />
        <span style={{ fontSize: '0.78rem', color: 'var(--text-secondary)' }}>Ctrl+Enter to run</span>
      </div>

      <div className="csvdb-explorer-layout">
        {/* Left: Schema sidebar */}
        <div className="csvdb-explorer-sidebar">
          <div className="csvdb-sidebar-tabs">
            <button className={`csvdb-stab ${schemaTab === 'schema' ? 'active' : ''}`} onClick={() => setSchemaTab('schema')}>
              <BarChart3 size={13} /> Schema
            </button>
            <button className={`csvdb-stab ${schemaTab === 'preview' ? 'active' : ''}`} onClick={() => setSchemaTab('preview')}>
              <Eye size={13} /> Preview
            </button>
          </div>

          {schemaTab === 'schema' && (
            <div className="csvdb-schema-list">
              {schemaLoading && <div style={{ padding: '1rem', color: 'var(--text-secondary)', textAlign: 'center' }}><Loader2 size={18} className="loading-spinner" /></div>}
              {Object.entries(schema).map(([tbl, info]) => (
                <div key={tbl} className={`csvdb-schema-tbl ${activeTable === tbl ? 'active' : ''}`}>
                  <div className="csvdb-schema-tbl-header" onClick={() => setActiveTable(activeTable === tbl ? null : tbl)}>
                    <Table2 size={13} style={{ color: 'var(--accent-primary)', flexShrink: 0 }} />
                    <span className="csvdb-schema-tbl-name">{tbl}</span>
                    <span className="csvdb-schema-tbl-count">{fmtRows(info.row_count)}</span>
                    <div style={{ display: 'flex', gap: '4px', marginLeft: 'auto' }}>
                      <button
                        className="csvdb-icon-btn" title="Quick SELECT"
                        onClick={e => { e.stopPropagation(); quickSelect(tbl); }}
                      ><Play size={11} /></button>
                      <button
                        className="csvdb-icon-btn" title="Preview data"
                        onClick={e => { e.stopPropagation(); loadPreview(tbl); }}
                      ><Eye size={11} /></button>
                    </div>
                  </div>
                  {activeTable === tbl && (
                    <div className="csvdb-schema-cols">
                      {info.columns.map(col => (
                        <div key={col.name} className="csvdb-schema-col-row">
                          <span className="csvdb-schema-col-name">{col.name}</span>
                          <TypeBadge type={col.type} />
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

          {schemaTab === 'preview' && (
            <div className="csvdb-preview-panel">
              {previewLoading && (
                <div style={{ padding: '1.5rem', textAlign: 'center' }}>
                  <Loader2 size={20} className="loading-spinner" style={{ color: 'var(--accent-primary)' }} />
                </div>
              )}
              {!previewLoading && !preview && (
                <div style={{ padding: '1rem', color: 'var(--text-secondary)', fontSize: '0.82rem', textAlign: 'center' }}>
                  Click <Eye size={12} style={{ display: 'inline', verticalAlign: 'middle' }} /> on a table to preview data.
                </div>
              )}
              {!previewLoading && preview && preview.error && (
                <div className="csvdb-error" style={{ margin: '0.75rem' }}>{preview.error}</div>
              )}
              {!previewLoading && preview && !preview.error && (
                <div>
                  <div style={{ padding: '0.5rem 0.75rem', fontSize: '0.78rem', color: 'var(--text-secondary)', borderBottom: '1px solid var(--border-color)' }}>
                    {preview.table} · {preview.row_count} rows shown
                  </div>
                  <ResultTable columns={preview.columns} rows={preview.rows} />
                </div>
              )}
            </div>
          )}
        </div>

        {/* Right: Editor + Results */}
        <div className="csvdb-explorer-main">
          {/* SQL Editor */}
          <div className="csvdb-editor-wrap">
            <div className="csvdb-editor-header">
              <span className="csvdb-editor-label"><TerminalSquare size={13} /> SQL Editor</span>
              <button className="csvdb-btn-run" onClick={runQuery} disabled={running}>
                {running ? <Loader2 size={14} className="loading-spinner" /> : <Play size={14} />}
                Run
              </button>
            </div>
            <textarea
              ref={textareaRef}
              className="csvdb-sql-editor"
              value={sql}
              onChange={e => setSql(e.target.value)}
              onKeyDown={handleKeyDown}
              spellCheck={false}
              placeholder="-- Write your SQL query here..."
            />
          </div>

          {/* Results */}
          <div className="csvdb-results-wrap">
            <div className="csvdb-results-header">
              <span>Results</span>
              {result && (
                <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
                  {result.row_count} row{result.row_count !== 1 ? 's' : ''}
                </span>
              )}
            </div>

            {runError && (
              <div className="csvdb-error" style={{ margin: '0.75rem' }}>
                <AlertCircle size={14} /> {runError}
              </div>
            )}

            {!result && !runError && (
              <div className="csvdb-results-empty">
                <TerminalSquare size={28} style={{ opacity: 0.35 }} />
                <p>Run a query to see results</p>
              </div>
            )}

            {result && result.columns.length > 0 && (
              <ResultTable columns={result.columns} rows={result.rows} />
            )}

            {result && result.columns.length === 0 && !runError && (
              <div className="csvdb-results-empty">
                <CheckCircle2 size={24} style={{ color: 'var(--success)' }} />
                <p>Query executed successfully — no rows returned.</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Root Component ─────────────────────────────────────────────────────────

export default function CsvDatabaseSection() {
  // view: 'list' | 'upload' | 'created' | 'explorer'
  const [view, setView]               = useState('list');
  const [createdResult, setCreatedResult] = useState(null);
  const [explorerDbId, setExplorerDbId]   = useState(null);
  const [explorerName, setExplorerName]   = useState('');
  const [listRefreshKey, setListRefreshKey] = useState(0);

  const handleCreated = (result) => {
    setCreatedResult(result);
    setView('created');
  };

  const handleExplore = (dbId) => {
    const name = createdResult?.company_name || explorerName;
    setExplorerDbId(dbId);
    setExplorerName(name);
    setView('explorer');
  };

  const handleSelectDb = (dbId, name) => {
    setExplorerDbId(dbId);
    setExplorerName(name);
    setView('explorer');
  };

  const handleBackToList = () => {
    setView('list');
    setListRefreshKey(k => k + 1);
  };

  return (
    <div className="csvdb-root">
      {view === 'list' && (
        <DatabaseList
          onSelect={handleSelectDb}
          onUpload={() => setView('upload')}
          refreshKey={listRefreshKey}
        />
      )}
      {view === 'upload' && (
        <div>
          <div style={{ padding: '1rem 1.5rem 0' }}>
            <button className="csvdb-btn-secondary csvdb-btn-sm" onClick={handleBackToList}>
              <ArrowLeft size={13} /> Back
            </button>
          </div>
          <UploadStep onCreated={handleCreated} />
        </div>
      )}
      {view === 'created' && createdResult && (
        <CreatedView
          result={createdResult}
          onExplore={handleExplore}
          onBack={handleBackToList}
        />
      )}
      {view === 'explorer' && explorerDbId && (
        <QueryExplorer
          dbId={explorerDbId}
          companyName={explorerName}
          onBack={handleBackToList}
        />
      )}
    </div>
  );
}
