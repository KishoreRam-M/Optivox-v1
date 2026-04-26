import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

/**
 * MarkdownRenderer
 * Renders markdown text with styled code blocks, tables, lists, etc.
 * Uses react-markdown + remark-gfm for GitHub-flavored markdown support.
 */
export default function MarkdownRenderer({ children, style = {} }) {
  if (!children) return null;

  return (
    <div className="md-body" style={style}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          // Inline code
          code({ node, inline, className, children: codeChildren, ...props }) {
            const match = /language-(\w+)/.exec(className || '');
            if (!inline) {
              return (
                <pre className="md-pre" {...props}>
                  <code className={`md-code-block ${className || ''}`}>
                    {String(codeChildren).replace(/\n$/, '')}
                  </code>
                </pre>
              );
            }
            return (
              <code className="md-inline-code" {...props}>
                {codeChildren}
              </code>
            );
          },
          // Tables
          table({ children: tableChildren }) {
            return (
              <div className="md-table-wrap">
                <table className="md-table">{tableChildren}</table>
              </div>
            );
          },
          th({ children: thChildren }) {
            return <th className="md-th">{thChildren}</th>;
          },
          td({ children: tdChildren }) {
            return <td className="md-td">{tdChildren}</td>;
          },
          // Headings
          h1({ children: hChildren }) {
            return <h1 className="md-h1">{hChildren}</h1>;
          },
          h2({ children: hChildren }) {
            return <h2 className="md-h2">{hChildren}</h2>;
          },
          h3({ children: hChildren }) {
            return <h3 className="md-h3">{hChildren}</h3>;
          },
          // Lists
          ul({ children: ulChildren }) {
            return <ul className="md-ul">{ulChildren}</ul>;
          },
          ol({ children: olChildren }) {
            return <ol className="md-ol">{olChildren}</ol>;
          },
          li({ children: liChildren }) {
            return <li className="md-li">{liChildren}</li>;
          },
          // Blockquote
          blockquote({ children: bqChildren }) {
            return <blockquote className="md-blockquote">{bqChildren}</blockquote>;
          },
          // Paragraphs
          p({ children: pChildren }) {
            return <p className="md-p">{pChildren}</p>;
          },
          // Horizontal rule
          hr() {
            return <hr className="md-hr" />;
          },
          // Strong / em
          strong({ children: sChildren }) {
            return <strong className="md-strong">{sChildren}</strong>;
          },
        }}
      >
        {children}
      </ReactMarkdown>
    </div>
  );
}
