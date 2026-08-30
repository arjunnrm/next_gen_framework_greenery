import React from "react";
import { sx } from "./sx.js";

// Faithful JSX port of the MetaFlow Spec Builder v4 reference. Every style
// literal is the mockup's string, unchanged.
export default function Shell({ V }) {
  return (
    <div style={sx("min-height:100vh;background:var(--bg);color:var(--tx);font:400 14px Inter,system-ui,sans-serif")}>

      <header style={sx("position:sticky;top:0;z-index:20;display:flex;flex-wrap:wrap;align-items:center;gap:10px 14px;padding:9px 18px;min-height:58px;box-sizing:border-box;min-width:0;background:var(--head);border-bottom:1px solid var(--bd)")}>
        <div style={sx("display:flex;align-items:baseline;gap:8px;flex:none;min-width:0")}>
          <span style={sx("font:700 15px Inter;letter-spacing:-.01em;white-space:nowrap")}>MetaFlow</span>
          <span style={sx("font:500 12px Inter;color:var(--dim2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis")}>Spec Builder</span>
        </div>
        <div style={sx("display:flex;align-items:center;gap:4px;flex:none;padding:3px;background:var(--seg);border:1px solid var(--bd);border-radius:8px")}>
          {V.flowTabs.map((t, i) => (
            <div key={i} onClick={t.go} style={sx(`padding:6px 12px;border-radius:6px;cursor:pointer;font:600 12px Inter;background:${t.bg};color:${t.fg}`)}>
              {t.label}
              <span style={sx("font:500 11px JetBrains Mono,monospace;color:var(--dim2);margin-left:6px")}>{t.count}</span>
            </div>
          ))}
        </div>
        <div style={sx("flex:1 1 20px")}></div>
        <div style={sx("display:flex;align-items:center;gap:9px;flex:none")}>
          <div style={sx("width:78px;height:5px;border-radius:3px;background:var(--bd3);overflow:hidden")}>
            <div style={sx(`height:100%;width:${V.progressPct};background:var(--ok);transition:width .25s`)}></div>
          </div>
          <span style={sx("font:600 11px JetBrains Mono,monospace;color:var(--ok);white-space:nowrap")}>{V.progressPct}</span>
          <span style={sx("font:500 11px Inter;color:var(--dim2);white-space:nowrap")}>{V.progressText}</span>
        </div>
        <div style={sx("display:flex;align-items:center;gap:6px;flex:none")}>
          <div onClick={V.showOpen} title="Open an existing spec from a file, Volume or Workspace path" style={sx("display:flex;align-items:center;gap:6px;padding:7px 13px;border-radius:7px;border:1px solid var(--acbd);background:var(--acfill);cursor:pointer;font:700 12px Inter;color:var(--ac2);white-space:nowrap")}>
            <span aria-hidden="true" style={sx("font:600 13px Inter;line-height:1")}>&#128194;</span>
            <span>Open spec</span>
          </div>
          <div onClick={V.openIndex} style={sx("padding:7px 11px;border-radius:7px;border:1px solid var(--bd);background:var(--btn);cursor:pointer;font:600 11.5px Inter;color:var(--tx2)")}>Index</div>
          <a href={V.docsHome} target="_blank" rel="noreferrer" style={sx("padding:7px 11px;border-radius:7px;border:1px solid var(--bd);background:var(--btn);font:600 11.5px Inter;color:var(--tx2)")}>Docs ↗</a>
          <div onClick={V.toggleTheme} title="Switch theme" style={sx("width:32px;height:31px;border-radius:7px;border:1px solid var(--bd);background:var(--btn);cursor:pointer;display:flex;align-items:center;justify-content:center;font:500 13px Inter;color:var(--tx2)")}>{V.themeIcon}</div>
          <div onClick={V.togglePreview} style={sx(`padding:7px 10px;white-space:nowrap;border-radius:7px;border:1px solid var(--bd);background:${V.previewBg};cursor:pointer;font:600 11.5px Inter;color:${V.previewFg}`)}>{V.previewLabel}</div>
        </div>
        <div onClick={V.startRun} style={sx("flex:none;padding:7px 16px;border-radius:7px;border:1px solid var(--acbd);background:var(--ac);cursor:pointer;font:700 12.5px Inter;color:#fff;box-shadow:0 2px 10px rgba(0,0,0,.25);display:flex;align-items:center;gap:6px;white-space:nowrap")}>
          <span>🚀</span>
          <span>Save / Onboard</span>
        </div>
        <div style={sx("display:flex;align-items:stretch;border:1px solid var(--acbd);border-radius:7px;overflow:hidden;flex:none")}>
          <div onClick={V.save} style={sx("padding:7px 12px;background:var(--acfill);cursor:pointer;font:600 12px Inter;color:var(--ac2);white-space:nowrap")}>Save .{V.fmt}</div>
          <div onClick={V.toggleFmt} title="Switch between JSON and YAML" style={sx("padding:7px 10px;background:var(--btn);cursor:pointer;font:600 11px Inter;color:var(--dim2);border-left:1px solid var(--bda2)")}>switch</div>
        </div>
      </header>

      <div style={sx(`display:grid;grid-template-columns:${V.gridCols};align-items:start`)}>

        <nav style={sx("position:sticky;top:var(--headerH);height:calc(100vh - var(--headerH));box-sizing:border-box;overflow-y:auto;border-right:1px solid var(--bd);background:var(--rail);padding:16px 14px 40px")}>
          <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:10px")}>Spec</div>
          <div onClick={V.selectRoot} style={sx(`padding:9px 11px;border-radius:8px;cursor:pointer;background:${V.rootBg};border:1px solid ${V.rootBd};margin-bottom:6px`)}>
            <div style={sx("font:600 12px Inter;color:var(--tx)")}>Spec root</div>
            <div style={sx("font:500 11px JetBrains Mono,monospace;color:var(--dim2);margin-top:2px")}>{V.groupId}</div>
          </div>
          <div style={sx("margin-bottom:18px")}></div>

          <div style={sx("display:flex;align-items:center;justify-content:space-between;margin-bottom:3px")}>
            <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3)")}>{V.flowKindLabel}</div>
            <div onClick={V.openAdd} style={sx("width:20px;height:20px;border-radius:5px;border:1px solid var(--bd5);display:flex;align-items:center;justify-content:center;cursor:pointer;font:600 13px Inter;color:var(--ac2)")}>+</div>
          </div>
          <div style={sx("font:400 10.5px Inter;color:var(--dim3);margin-bottom:9px")}>in {V.groupId}</div>
          <div style={sx("display:flex;flex-direction:column;gap:5px")}>
            {V.flowList.map((fl, i) => (
              <div key={i} onClick={fl.go} style={sx(`padding:9px 11px;border-radius:8px;cursor:pointer;background:${fl.bg};border:1px solid ${fl.bd}`)}>
                <div style={sx("display:flex;align-items:center;gap:7px")}>
                  <span style={sx(`flex:1;min-width:0;font:600 11.5px JetBrains Mono,monospace;color:${fl.fg};overflow:hidden;text-overflow:ellipsis;white-space:nowrap`)}>{fl.id}</span>
                  <span onClick={fl.del} style={sx("flex:none;font:500 13px Inter;color:var(--dim3);cursor:pointer")}>×</span>
                </div>
                <div style={sx("display:flex;align-items:center;gap:7px;margin-top:3px")}>
                  <span style={sx("flex:1;min-width:0;font:500 10.5px Inter;color:var(--dim2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap")}>{fl.meta}</span>
                  <span style={sx(`flex:none;font:600 9.5px JetBrains Mono,monospace;color:${fl.pctFg}`)}>{fl.pct}</span>
                </div>
              </div>
            ))}
          </div>

          <div style={sx("margin-top:22px;border-top:1px solid var(--bd3);padding-top:14px")}>
            <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:9px")}>Sections</div>
            <div style={sx("display:flex;flex-direction:column;gap:1px")}>
              {V.navSections.map((n, i) => (
                <a key={i} href={n.href} style={sx("display:flex;align-items:center;gap:8px;padding:5px 6px;border-radius:6px;font:500 11.5px Inter;color:var(--dim)")}>
                  <span style={sx("flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap")}>{n.title}</span>
                  <span style={sx("font:500 10px JetBrains Mono,monospace;color:var(--dim4)")}>{n.count}</span>
                </a>
              ))}
            </div>
          </div>

          <div style={sx("margin-top:20px;border-top:1px solid var(--bd3);padding-top:14px")}>
            <div onClick={V.toggleInert} style={sx("display:flex;align-items:center;gap:9px;cursor:pointer")}>
              <span style={sx(`flex:none;width:32px;height:18px;border-radius:9px;background:${V.inertTrack};position:relative;transition:background .15s`)}>
                <span style={sx(`position:absolute;top:2px;left:${V.inertKnob};width:14px;height:14px;border-radius:50%;background:var(--tx);transition:left .15s`)}></span>
              </span>
              <span style={sx("font:500 11.5px Inter;color:var(--dim)")}>Show attributes not applicable</span>
            </div>
          </div>

          {V.accessRows.length > 0 && (
            <div style={sx("margin-top:20px;border-top:1px solid var(--bd3);padding-top:14px")}>
              <div style={sx("display:flex;align-items:center;gap:8px;margin-bottom:9px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);flex:1")}>Access</div>
                <div onClick={V.recheckAccess} style={sx("font:600 10px Inter;color:var(--ac2);cursor:pointer")}>re-check</div>
              </div>
              <div style={sx("display:flex;flex-direction:column;gap:4px")}>
                {V.accessRows.map((a, i) => (
                  <div key={i} title={a.remediation} style={sx("display:flex;align-items:center;gap:8px")}>
                    <span style={sx(`flex:none;width:6px;height:6px;border-radius:50%;background:${a.dot}`)}></span>
                    <span style={sx("flex:1;min-width:0;font:500 10.5px Inter;color:var(--dim2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap")}>{a.label}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </nav>

        <main style={sx("padding:24px 30px 90px;min-width:0")}>
          <div style={sx("display:flex;align-items:flex-end;gap:14px;margin-bottom:4px")}>
            <h1 style={sx("margin:0;font:600 20px Inter;letter-spacing:-.01em")}>{V.pageTitle}</h1>
            <span style={sx("font:500 12px JetBrains Mono,monospace;color:var(--dim2);padding-bottom:3px")}>{V.pageKind}</span>
            <span style={sx("flex:1")}></span>
            <span style={sx("font:500 11.5px Inter;color:var(--ok);padding-bottom:3px")}>{V.flowProgress}</span>
          </div>
          <div style={sx("font:400 12.5px/1.5 Inter;color:var(--dim2);max-width:70ch;margin-bottom:18px")}>{V.pageSub}</div>

          <div style={sx("display:flex;align-items:center;gap:7px;flex-wrap:wrap;margin-bottom:20px")}>
            {V.phases.map((p, i) => (
              <div key={i} onClick={p.go} style={sx(`display:flex;align-items:center;gap:8px;padding:7px 12px 7px 10px;border-radius:9px;cursor:pointer;background:${p.bg};border:1px solid ${p.bd}`)}>
                <span style={sx(`flex:none;width:7px;height:7px;border-radius:50%;background:${p.dot}`)}></span>
                <span style={sx("display:flex;flex-direction:column;gap:1px")}>
                  <span style={sx(`font:600 11.5px Inter;color:${p.fg};white-space:nowrap`)}>{p.label}</span>
                  <span style={sx("font:500 9.5px JetBrains Mono,monospace;color:var(--dim3);white-space:nowrap")}>{p.meta}</span>
                </span>
              </div>
            ))}
          </div>

          <div style={sx("display:flex;flex-wrap:wrap;gap:20px")}>
            {V.sections.map((s) => (
              <section key={s.id} id={s.id} data-screen-label={s.title} style={sx(`flex:${s.flex};box-sizing:border-box;min-width:${s.minw};background:var(--panel);border:1px solid var(--bd2);border-radius:12px;padding:18px 20px 22px`)}>
                <div style={sx("display:flex;align-items:center;gap:10px;margin-bottom:5px")}>
                  <span style={sx("font:600 10px JetBrains Mono,monospace;color:var(--dim4)")}>{s.num}</span>
                  <span style={sx(`font:600 13.5px Inter;opacity:${s.titleOp}`)}>{s.title}</span>
                  <a href={s.doc} target="_blank" rel="noreferrer" style={sx("font:500 10.5px Inter;color:var(--ac)")}>{s.docLabel}</a>
                  <span style={sx("flex:1")}></span>
                  <span style={sx("font:500 10.5px JetBrains Mono,monospace;color:var(--dim4)")}>{s.count}</span>
                </div>
                <div style={sx("font:400 12px/1.55 Inter;color:var(--dim2);margin-bottom:16px;max-width:78ch")}>{s.sub}</div>

                {s.isTabs && (
                  <div>
                    <div style={sx("display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px")}>
                      {V.cdcTabs.map((c, i) => (
                        <div key={i} onClick={c.go} style={sx(`flex:none;display:flex;flex-direction:column;gap:2px;padding:9px 14px;cursor:${c.cursor};background:${c.bg};border:1px solid ${c.bd};border-radius:9px;opacity:${c.op}`)}>
                          <span style={sx(`font:600 11.5px JetBrains Mono,monospace;color:${c.fg};white-space:nowrap`)}>{c.v}</span>
                          <span style={sx("font:500 9.5px Inter;color:var(--dim3);white-space:nowrap")}>{c.badge}</span>
                        </div>
                      ))}
                    </div>
                    <div style={sx("border:1px solid var(--bd4);border-radius:11px;background:var(--panel3);padding:16px 17px 18px")}>
                      <div style={sx("display:flex;align-items:center;gap:10px;margin-bottom:4px")}>
                        <span style={sx("font:600 12.5px Inter")}>{V.cdcCur}</span>
                        <span style={sx("font:600 9.5px Inter;letter-spacing:.05em;text-transform:uppercase;color:var(--req)")}>{V.cdcParams}</span>
                      </div>
                      <div style={sx("font:400 11.5px/1.5 Inter;color:var(--dim2);margin-bottom:15px")}>{V.cdcDesc}</div>
                      <div style={sx("display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:15px 18px")}>
                        {V.cdcFields.map((f, i) => (
                          <Field key={i} f={f} />
                        ))}
                      </div>
                    </div>
                  </div>
                )}

                <div style={sx(`display:grid;grid-template-columns:${s.colsCss};gap:16px 18px`)}>
                  {s.fields.map((f, i) => (
                    <Field key={i} f={f} />
                  ))}
                </div>
              </section>
            ))}
          </div>

          <div style={sx("display:flex;align-items:center;gap:12px;margin-top:26px")}>
            <div onClick={V.prevPhase} style={sx(`padding:9px 15px;border-radius:8px;border:1px solid var(--bdi);background:var(--btn);cursor:pointer;font:600 11.5px Inter;color:var(--tx2);opacity:${V.prevOp}`)}>← Back</div>
            <span style={sx("font:500 11px JetBrains Mono,monospace;color:var(--dim3)")}>{V.phaseStepText}</span>
            <span style={sx("flex:1")}></span>
            <div onClick={V.nextPhase} style={sx(`padding:9px 15px;border-radius:8px;border:1px solid var(--acbd);background:var(--acfill);cursor:pointer;font:600 11.5px Inter;color:var(--ac2);opacity:${V.nextOp}`)}>{V.nextLabel} →</div>
          </div>
        </main>

        {V.showPreview && (
          <aside style={sx("position:sticky;top:var(--headerH);height:calc(100vh - var(--headerH));border-left:1px solid var(--bd);background:var(--rail);display:flex;flex-direction:column")}>
            <div style={sx("display:flex;align-items:center;gap:10px;padding:13px 16px;border-bottom:1px solid var(--bd3)")}>
              <span style={sx("font:600 11.5px Inter")}>Spec preview</span>
              <span style={sx("flex:1")}></span>
              <div style={sx("display:flex;gap:2px;padding:2px;background:var(--seg);border:1px solid var(--bd);border-radius:7px")}>
                <div onClick={V.setJson} style={sx(`padding:4px 10px;border-radius:5px;cursor:pointer;font:600 10.5px JetBrains Mono,monospace;background:${V.jsonBg};color:${V.jsonFg}`)}>JSON</div>
                <div onClick={V.setYaml} style={sx(`padding:4px 10px;border-radius:5px;cursor:pointer;font:600 10.5px JetBrains Mono,monospace;background:${V.yamlBg};color:${V.yamlFg}`)}>YAML</div>
              </div>
              <div onClick={V.copy} style={sx("padding:5px 10px;border-radius:6px;border:1px solid var(--bd);cursor:pointer;font:600 10.5px Inter;color:var(--ac2)")}>{V.copyLabel}</div>
            </div>
            <pre style={sx("flex:1;overflow:auto;margin:0;padding:14px 16px 40px;font:400 11px/1.6 JetBrains Mono,monospace;color:var(--tx4);white-space:pre")}>{V.preview}</pre>
          </aside>
        )}
      </div>

      {V.info && (
        <div style={sx("position:fixed;inset:0;z-index:60;background:var(--ovl);display:flex;justify-content:flex-end")}>
          <div onClick={V.closeInfo} style={sx("flex:1")}></div>
          <div style={sx("width:430px;height:100%;overflow-y:auto;background:var(--panel);border-left:1px solid var(--bd4);padding:22px 24px 60px")}>
            <div style={sx("display:flex;align-items:flex-start;gap:12px;margin-bottom:18px")}>
              <div style={sx("flex:1;min-width:0")}>
                <div style={sx("font:600 13.5px JetBrains Mono,monospace;color:var(--tx);word-break:normal;overflow-wrap:anywhere")}>{V.infoName}</div>
                <div style={sx("font:500 11px Inter;color:var(--dim2);margin-top:4px")}>{V.infoSection}</div>
              </div>
              <div onClick={V.closeInfo} style={sx("flex:none;width:26px;height:26px;border-radius:7px;border:1px solid var(--bdi);display:flex;align-items:center;justify-content:center;cursor:pointer;color:var(--dim2)")}>×</div>
            </div>
            {V.infoInert && (
              <div style={sx("display:flex;gap:9px;padding:10px 12px;border-radius:9px;border:1px solid var(--bd5);background:var(--panel2);margin-bottom:16px")}>
                <span style={sx("flex:none;font:600 11px Inter;color:var(--req)")}>◍</span>
                <span style={sx("font:400 11.5px/1.55 Inter;color:var(--tx3)")}>
                  <b style={sx("font-weight:600;color:var(--req)")}>Not applicable right now</b> — {V.infoInertReason}. It stays in the
                  spec editor so you can see it exists, but it will not be written to the spec.
                </span>
              </div>
            )}

            {V.infoHasKnow && V.infoKnow.purpose && (
              <div style={sx("margin-bottom:16px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:6px")}>What it does</div>
                <div style={sx("font:400 12.5px/1.7 Inter;color:var(--tx2)")}>{V.infoKnow.purpose}</div>
              </div>
            )}
            {V.infoHasKnow && V.infoKnow.why && (
              <div style={sx("margin-bottom:16px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:6px")}>Why it matters</div>
                <div style={sx("font:400 12.5px/1.7 Inter;color:var(--tx3)")}>{V.infoKnow.why}</div>
              </div>
            )}

            <div style={sx("font:400 12.5px/1.7 Inter;color:var(--tx2);margin-bottom:16px")}>{V.infoDesc}</div>

            {V.infoHint && (
              <div style={sx("padding:9px 12px;border-radius:8px;border:1px dashed var(--bd5);margin-bottom:16px;font:400 11.5px/1.55 Inter;color:var(--dim)")}>
                {V.infoHint}
              </div>
            )}

            <div style={sx("display:flex;flex-direction:column;gap:1px;border:1px solid var(--bd2);border-radius:9px;overflow:hidden;margin-bottom:18px")}>
              {V.infoCurrentShown && (
                <div style={sx("display:grid;grid-template-columns:104px minmax(0,1fr);gap:12px;padding:9px 12px;background:var(--sel)")}>
                  <span style={sx("font:600 10.5px Inter;letter-spacing:.04em;text-transform:uppercase;color:var(--ac2)")}>current</span>
                  <span style={sx("font:400 11.5px/1.5 JetBrains Mono,monospace;color:var(--ac2);word-break:break-word")}>{V.infoCurrent}</span>
                </div>
              )}
              {V.infoRows.map((r, i) => (
                <div key={i} style={sx("display:grid;grid-template-columns:104px minmax(0,1fr);gap:12px;padding:9px 12px;background:var(--panel3)")}>
                  <span style={sx("font:600 10.5px Inter;letter-spacing:.04em;text-transform:uppercase;color:var(--dim3)")}>{r.k}</span>
                  <span style={sx("font:400 11.5px/1.5 JetBrains Mono,monospace;color:var(--tx2);word-break:break-word")}>{r.v}</span>
                </div>
              ))}
            </div>

            {V.infoEnum.length > 0 && (
              <div style={sx("margin-bottom:18px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>Allowed values</div>
                <div style={sx("display:flex;flex-direction:column;gap:6px")}>
                  {V.infoEnum.map((e, i) => (
                    <div key={i} style={sx("padding:8px 11px;border-radius:8px;border:1px solid var(--bd3);background:var(--panel3)")}>
                      <span style={sx("font:600 11.5px JetBrains Mono,monospace;color:var(--tx2)")}>{e.v}</span>
                      {e.note && <span style={sx("display:block;font:400 11px/1.5 Inter;color:var(--dim2);margin-top:3px")}>{e.note}</span>}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {V.infoChildren.length > 0 && (
              <div style={sx("margin-bottom:18px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>Fields inside each entry</div>
                <div style={sx("display:flex;flex-direction:column;gap:6px")}>
                  {V.infoChildren.map((c, i) => (
                    <div key={i} style={sx("padding:8px 11px;border-radius:8px;border:1px solid var(--bd3);background:var(--panel3)")}>
                      <span style={sx("font:600 11.5px JetBrains Mono,monospace;color:var(--tx2)")}>{c.p}</span>
                      {c.req && <span style={sx("font:600 8.5px Inter;letter-spacing:.07em;color:var(--req);margin-left:7px")}>REQUIRED</span>}
                      {c.i && <span style={sx("display:block;font:400 11px/1.5 Inter;color:var(--dim2);margin-top:3px")}>{c.i}</span>}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {V.infoRelated.length > 0 && (
              <div style={sx("margin-bottom:20px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>Set alongside this</div>
                <div style={sx("display:flex;flex-wrap:wrap;gap:5px")}>
                  {V.infoRelated.map((r, i) => (
                    <span key={i} style={sx("padding:3px 8px;border-radius:6px;border:1px solid var(--bd3);background:var(--panel3);font:500 10.5px JetBrains Mono,monospace;color:var(--dim)")}>{r}</span>
                  ))}
                </div>
              </div>
            )}

            {V.infoHasKnow && V.infoKnow.samples.length > 0 && (
              <div style={sx("margin-bottom:20px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>Sample values</div>
                {V.infoKnow.samples.map((sm, i) => (
                  <pre key={i} style={sx("margin:0 0 7px;padding:11px 12px;border-radius:9px;border:1px solid var(--bd3);background:var(--panel2);overflow-x:auto;font:400 11px/1.6 JetBrains Mono,monospace;color:var(--tx2);white-space:pre")}>{sm.code}</pre>
                ))}
              </div>
            )}

            {V.infoHasKnow && V.infoKnow.tips.length > 0 && (
              <div style={sx("margin-bottom:20px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>Best practice</div>
                <div style={sx("display:flex;flex-direction:column;gap:6px")}>
                  {V.infoKnow.tips.map((t, i) => (
                    <div key={i} style={sx("display:flex;gap:8px;font:400 11.5px/1.6 Inter;color:var(--tx3)")}>
                      <span style={sx("flex:none;color:var(--ok);font:600 11px Inter")}>✓</span>
                      <span>{t}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {V.infoHasKnow && V.infoKnow.errors.length > 0 && (
              <div style={sx("margin-bottom:20px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>Known errors &amp; limitations</div>
                <div style={sx("display:flex;flex-direction:column;gap:8px")}>
                  {V.infoKnow.errors.map((e, i) => (
                    <div key={i} style={sx("padding:10px 12px;border-radius:9px;border:1px solid var(--bd3);background:var(--panel2)")}>
                      <div style={sx("font:600 11.5px/1.5 Inter;color:var(--req)")}>{e.symptom}</div>
                      {e.cause && <div style={sx("font:400 11px/1.55 Inter;color:var(--dim2);margin-top:4px")}><b style={sx("font-weight:600")}>Cause:</b> {e.cause}</div>}
                      {e.fix && <div style={sx("font:400 11px/1.55 Inter;color:var(--tx3);margin-top:3px")}><b style={sx("font-weight:600")}>Fix:</b> {e.fix}</div>}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {V.infoHasKnow && V.infoKnow.refs.length > 0 && (
              <div style={sx("margin-bottom:20px")}>
                <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>Databricks documentation</div>
                <div style={sx("display:flex;flex-direction:column;gap:5px")}>
                  {V.infoKnow.refs.map((r, i) => (
                    <a key={i} href={r.url} target="_blank" rel="noreferrer" style={sx("padding:7px 11px;border-radius:8px;border:1px solid var(--bd3);background:var(--panel3);font:500 11px Inter;color:var(--ac2);text-decoration:none;word-break:break-all")}>{r.label} ↗</a>
                  ))}
                </div>
              </div>
            )}

            {V.infoDocShown && (
              <a href={V.infoDoc} target="_blank" rel="noreferrer" style={sx("display:inline-flex;padding:8px 13px;border-radius:8px;border:1px solid var(--bda);background:var(--acbtn);font:600 11.5px Inter;color:var(--ac2)")}>Full reference for this section ↗</a>
            )}
          </div>
        </div>
      )}

      {V.openOpen && (
        <div style={sx("position:fixed;inset:0;z-index:60;background:var(--ovl);display:flex;align-items:center;justify-content:center;padding:40px")}>
          <div style={sx("width:720px;max-width:100%;max-height:100%;overflow-y:auto;background:var(--panel);border:1px solid var(--bd4);border-radius:14px;padding:22px 24px 24px")}>
            <div style={sx("display:flex;align-items:center;gap:12px;margin-bottom:6px")}>
              <span style={sx("font:600 14px Inter;color:var(--tx)")}>Open an existing spec</span>
              <span style={sx("flex:1")}></span>
              <div onClick={V.closeOpen} style={sx("width:26px;height:26px;border-radius:7px;border:1px solid var(--bdi);display:flex;align-items:center;justify-content:center;cursor:pointer;color:var(--dim2)")}>×</div>
            </div>
            <div style={sx("font:400 11.5px/1.6 Inter;color:var(--dim2);margin-bottom:18px")}>
              Loads the spec into the builder so you can edit every attribute and save it back.
              Anything the builder does not recognise is preserved, so a round trip does not drop fields.
            </div>

            <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>Upload a file</div>
            <label style={sx("display:flex;align-items:center;gap:10px;padding:12px 14px;border-radius:10px;border:1px dashed var(--bd5);background:var(--panel3);cursor:pointer;margin-bottom:20px")}>
              <span style={sx("font:600 11.5px Inter;color:var(--ac2)")}>Choose .json / .yaml…</span>
              <span style={sx("font:400 11px Inter;color:var(--dim3)")}>from this computer</span>
              <input type="file" accept=".json,.yaml,.yml,application/json,text/yaml" onChange={V.onLocalFile} style={sx("display:none")} />
            </label>

            <div style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>Browse Databricks storage</div>
            <div style={sx("display:flex;gap:7px;margin-bottom:12px")}>
              {V.openSources.map((d, i) => (
                <div key={i} onClick={d.go} style={sx(`flex:1;padding:10px 12px;border-radius:9px;border:1px solid ${d.bd};background:${d.bg};color:${d.fg};cursor:pointer;font:600 11.5px Inter`)}>{d.label}</div>
              ))}
            </div>

            <div style={sx("font:600 10.5px Inter;color:var(--dim3);margin-bottom:5px")}>Path</div>
            <div style={sx("display:flex;gap:7px;margin-bottom:9px")}>
              <input value={V.openPath} onChange={V.onOpenPath} placeholder={V.openPathPlaceholder} style={sx("flex:1;min-width:0;box-sizing:border-box;height:34px;padding:0 10px;background:var(--input);border:1px solid var(--bdi);border-radius:7px;color:var(--tx);font:400 11.5px JetBrains Mono,monospace")} />
              <div onClick={V.browseHere} title="List this directory" style={sx("flex:none;padding:0 13px;height:34px;display:flex;align-items:center;border-radius:7px;border:1px solid var(--bdi);background:var(--btn);cursor:pointer;font:600 11.5px Inter;color:var(--tx2)")}>Browse</div>
              <div onClick={V.checkPath} title="Read, parse and validate this file without opening it" style={sx("flex:none;padding:0 13px;height:34px;display:flex;align-items:center;border-radius:7px;border:1px solid var(--bdi);background:var(--btn);cursor:pointer;font:600 11.5px Inter;color:var(--tx2)")}>{V.openChecking ? "Checking…" : "Validate"}</div>
              <div onClick={V.openTypedPath} style={sx("flex:none;padding:0 13px;height:34px;display:flex;align-items:center;border-radius:7px;border:1px solid var(--acbd);background:var(--acfill);cursor:pointer;font:700 11.5px Inter;color:var(--ac2)")}>Open</div>
            </div>
            <div style={sx("font:400 10px/1.5 Inter;color:var(--dim3);margin-bottom:10px")}>
              Leave blank to list the configured root. Absolute paths are accepted — Volumes must start
              <code style={sx("font:500 10px JetBrains Mono,monospace;color:var(--dim2)")}> /Volumes/</code>, workspace paths
              <code style={sx("font:500 10px JetBrains Mono,monospace;color:var(--dim2)")}> /Workspace/</code>.
            </div>

            {V.openDir && (
              <div style={sx("font:500 10.5px JetBrains Mono,monospace;color:var(--dim3);margin-bottom:8px;word-break:break-all")}>{V.openDir}</div>
            )}

            {V.openCheck && (
              <div style={sx(`padding:11px 13px;border-radius:9px;border:1px solid ${V.openCheck.ok ? "var(--ok)" : "var(--req)"};background:var(--panel2);margin-bottom:11px`)}>
                <div style={sx(`font:700 11.5px Inter;color:${V.openCheck.ok ? "var(--ok)" : "var(--req)"};margin-bottom:5px`)}>{V.openCheck.headline}</div>
                {V.openCheck.detail && (
                  <div style={sx("font:400 11px/1.5 JetBrains Mono,monospace;color:var(--tx3);margin-bottom:6px;word-break:break-word")}>{V.openCheck.detail}</div>
                )}
                <div style={sx("display:flex;flex-wrap:wrap;gap:4px;margin-bottom:6px")}>
                  {V.openCheck.summary.map((r, i) => (
                    <span key={i} style={sx("padding:2px 7px;border-radius:5px;border:1px solid var(--bd3);background:var(--panel3);font:500 10px JetBrains Mono,monospace;color:var(--dim)")}>{r[0]}: {r[1]}</span>
                  ))}
                </div>
                {V.openCheck.errors.map((e, i) => (
                  <div key={i} style={sx("font:400 10.5px/1.5 Inter;color:var(--req)")}>• {e}</div>
                ))}
                {V.openCheck.more > 0 && (
                  <div style={sx("font:400 10.5px Inter;color:var(--dim3);margin-top:3px")}>…and {V.openCheck.more} more</div>
                )}
                {V.openCheck.warnings.map((e, i) => (
                  <div key={i} style={sx("font:400 10.5px/1.5 Inter;color:var(--dim2)")}>▲ {e}</div>
                ))}
              </div>
            )}

            {V.openUp && (
              <div onClick={V.openUp.go} style={sx("display:flex;align-items:center;gap:8px;padding:8px 12px;border-radius:8px;border:1px solid var(--bd4);background:var(--panel2);cursor:pointer;margin-bottom:6px;font:500 11px JetBrains Mono,monospace;color:var(--dim)")}>
                <span style={sx("font:600 11px Inter")}>↑</span> {V.openUp.label}
              </div>
            )}
            {V.openDirs.length > 0 && (
              <div style={sx("display:flex;flex-direction:column;gap:5px;margin-bottom:8px")}>
                {V.openDirs.map((d, i) => (
                  <div key={i} onClick={d.go} style={sx("display:flex;align-items:center;gap:9px;padding:8px 12px;border-radius:8px;border:1px solid var(--bd4);background:var(--panel2);cursor:pointer")}>
                    <span aria-hidden="true">&#128193;</span>
                    <span style={sx("flex:1;min-width:0;font:600 11.5px JetBrains Mono,monospace;color:var(--tx3);word-break:break-all")}>{d.name}</span>
                    <span style={sx("font:600 10px Inter;color:var(--dim3)")}>OPEN DIR</span>
                  </div>
                ))}
              </div>
            )}
            {V.openBusy && <div style={sx("font:500 11.5px Inter;color:var(--ac2);padding:10px 0")}>Loading…</div>}
            {V.openErr && (
              <div style={sx("padding:10px 12px;border-radius:8px;border:1px solid var(--req);background:var(--panel2);font:400 11.5px/1.5 Inter;color:var(--req);margin-bottom:10px")}>{V.openErr}</div>
            )}
            {V.openEmpty && !V.openBusy && (
              <div style={sx("font:400 11.5px Inter;color:var(--dim3);padding:10px 0")}>No spec files found in this root.</div>
            )}
            {V.openListed && V.openFiles.length > 0 && (
              <div style={sx("display:flex;flex-direction:column;gap:6px;max-height:280px;overflow-y:auto")}>
                {V.openFiles.map((f, i) => (
                  <div key={i} onClick={f.go} style={sx("display:flex;gap:12px;align-items:center;padding:10px 13px;border-radius:9px;border:1px solid var(--bd4);background:var(--panel3);cursor:pointer")}>
                    <span style={sx("flex:1;min-width:0")}>
                      <span style={sx("display:block;font:600 11.5px JetBrains Mono,monospace;color:var(--tx2);word-break:break-all")}>{f.name}</span>
                      {f.meta && <span style={sx("display:block;font:400 10.5px Inter;color:var(--dim3);margin-top:2px")}>{f.meta}</span>}
                    </span>
                    <span style={sx("flex:none;font:600 10px Inter;letter-spacing:.06em;color:var(--ac)")}>OPEN</span>
                  </div>
                ))}
              </div>
            )}

            <div onClick={V.closeOpen} style={sx("margin-top:18px;display:inline-flex;padding:8px 14px;border-radius:8px;border:1px solid var(--bdi);cursor:pointer;font:600 11.5px Inter;color:var(--dim)")}>Cancel</div>
          </div>
        </div>
      )}

      {V.addOpen && (
        <div style={sx("position:fixed;inset:0;z-index:60;background:var(--ovl);display:flex;align-items:center;justify-content:center;padding:40px")}>
          <div style={sx("width:660px;max-width:100%;max-height:100%;overflow-x:hidden;overflow-y:auto;box-sizing:border-box;background:var(--panel);border:1px solid var(--bd4);border-radius:14px;padding:22px 24px 24px")}>
            <div style={sx("display:flex;align-items:baseline;gap:12px;margin-bottom:5px")}>
              <span style={sx("font:600 14px Inter")}>Add {V.flowKindSingular}</span>
              <span style={sx("flex:1")}></span>
              <span style={sx("flex:none;font:500 11px Inter;color:var(--dim2);white-space:nowrap")}>{V.dialogProgress}</span>
            </div>
            <div style={sx("font:400 12px/1.5 Inter;color:var(--dim2);margin-bottom:16px")}>Create a new flow for the full attribute set, clone one you already have, or start from a known-good combination out of the onboarding template.</div>

            <div style={sx("display:flex;flex-wrap:wrap;gap:10px;margin-bottom:18px")}>
              <div onClick={V.startScratch} style={sx("flex:1 1 270px;min-width:0;padding:15px 16px;border-radius:11px;border:1px solid var(--acbd);background:var(--acfill);cursor:pointer")}>
                <div style={sx("display:flex;align-items:baseline;gap:8px")}>
                  <span style={sx("flex:1;font:600 13px Inter;color:var(--tx)")}>{V.scratchLabel}</span>
                  <span style={sx("flex:none;font:600 10px Inter;letter-spacing:.06em;color:var(--ac2)")}>START</span>
                </div>
                <div style={sx("font:400 11.5px/1.5 Inter;color:var(--dim);margin-top:4px")}>{V.scratchDesc}</div>
              </div>
              <div style={sx(`flex:1 1 270px;min-width:0;padding:15px 16px;border-radius:11px;border:1px solid var(--bd4);background:var(--panel3);opacity:${V.cloneOp}`)}>
                <div style={sx("display:flex;align-items:baseline;gap:8px")}>
                  <span style={sx("flex:1;font:600 13px Inter;color:var(--tx)")}>Clone from existing</span>
                  <span style={sx("flex:none;font:600 10px Inter;letter-spacing:.06em;color:var(--dim3)")}>COPY</span>
                </div>
                <div style={sx("font:400 11.5px/1.5 Inter;color:var(--dim);margin-top:4px")}>{V.cloneDesc}</div>
                <div style={sx("display:flex;gap:7px;margin-top:11px")}>
                  <select value={V.cloneFrom} onChange={V.onCloneFrom} style={sx("flex:1;min-width:0;box-sizing:border-box;height:32px;padding:0 8px;background:var(--input);border:1px solid var(--bdi);border-radius:7px;color:var(--tx);font:400 11px JetBrains Mono,monospace")}>
                    {V.cloneOptions.map((c, i) => (
                      <option key={i} value={c.v}>{c.t}</option>
                    ))}
                  </select>
                  <div onClick={V.doClone} style={sx("flex:none;padding:0 13px;height:32px;display:flex;align-items:center;border-radius:7px;border:1px solid var(--bdi);background:var(--btn);cursor:pointer;font:600 11px Inter;color:var(--tx2)")}>Clone</div>
                </div>
              </div>
            </div>

            <div style={sx("display:flex;align-items:center;gap:11px;margin-bottom:11px")}>
              <span style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3)")}>Browse templates</span>
              <span style={sx("font:500 10.5px JetBrains Mono,monospace;color:var(--dim4)")}>{V.presetCount}</span>
              <span style={sx("flex:1")}></span>
              <input value={V.tplQuery} onChange={V.onTplQuery} placeholder="search templates…" style={sx("width:200px;box-sizing:border-box;height:31px;padding:0 10px;background:var(--input);border:1px solid var(--bdi);border-radius:7px;color:var(--tx);font:400 11.5px Inter")} />
            </div>
            <div style={sx("display:flex;flex-direction:column;gap:8px")}>
              {V.presets.map((p, i) => (
                <div key={i} onClick={p.go} style={sx("display:flex;gap:12px;align-items:flex-start;padding:13px 14px;border-radius:10px;border:1px solid var(--bd4);background:var(--panel3);cursor:pointer")}>
                  <span style={sx("flex:1;min-width:0")}>
                    <span style={sx("display:block;font:600 12px JetBrains Mono,monospace;color:var(--tx)")}>{p.name}</span>
                    <span style={sx("display:block;font:400 11.5px/1.5 Inter;color:var(--dim2);margin-top:3px")}>{p.desc}</span>

                    {p.chips.length > 0 && (
                      <span style={sx("display:flex;flex-wrap:wrap;gap:4px;margin-top:8px")}>
                        {p.chips.map((c, j) => (
                          <span key={j} style={sx("padding:2px 7px;border-radius:5px;border:1px solid var(--bda2);background:var(--acbtn);font:500 10px JetBrains Mono,monospace;color:var(--ac2)")}>{c}</span>
                        ))}
                      </span>
                    )}

                    {p.feats.length > 0 && (
                      <span style={sx("display:flex;flex-wrap:wrap;gap:4px;margin-top:4px")}>
                        {p.feats.map((c, j) => (
                          <span key={j} style={sx("padding:2px 7px;border-radius:5px;border:1px solid var(--bd3);background:var(--panel2);font:500 10px Inter;color:var(--dim)")}>{c}</span>
                        ))}
                      </span>
                    )}

                    <span style={sx("display:block;font:500 10px Inter;color:var(--dim3);margin-top:7px")}>{p.count} · everything stays editable afterwards</span>
                  </span>
                  <span style={sx("flex:none;font:600 10px Inter;letter-spacing:.06em;color:var(--ac)")}>ADD</span>
                </div>
              ))}
            </div>
            {V.discoveredTemplates.length > 0 && (
              <div style={sx("margin-top:20px")}>
                <div style={sx("display:flex;align-items:center;gap:11px;margin-bottom:9px")}>
                  <span style={sx("font:600 10px Inter;letter-spacing:.1em;text-transform:uppercase;color:var(--dim3)")}>From the templates folder</span>
                  <span style={sx("font:500 10.5px JetBrains Mono,monospace;color:var(--dim4)")}>{V.discoveredCount}</span>
                </div>
                <div style={sx("font:400 10.5px/1.5 Inter;color:var(--dim3);margin-bottom:9px")}>
                  Discovered by scanning <code style={sx("font:500 10px JetBrains Mono,monospace;color:var(--dim2)")}>databricks-app/templates/</code> —
                  drop a .json file in there and it appears here on the next load, with no rebuild.
                </div>
                <div style={sx("display:flex;flex-direction:column;gap:8px")}>
                  {V.discoveredTemplates.map((p, i) => (
                    <div key={i} onClick={p.error ? undefined : p.go} style={sx(`display:flex;gap:12px;align-items:flex-start;padding:12px 14px;border-radius:10px;border:1px solid var(--bd4);background:var(--panel3);cursor:${p.error ? "not-allowed" : "pointer"};opacity:${p.error ? "0.6" : "1"}`)}>
                      <span style={sx("flex:1;min-width:0")}>
                        <span style={sx("display:block;font:600 12px JetBrains Mono,monospace;color:var(--tx);word-break:break-all")}>
                          {p.name}
                          {p.isSpec && <span style={sx("font:600 8.5px Inter;letter-spacing:.06em;color:var(--ac2);margin-left:7px")}>WHOLE SPEC</span>}
                          {p.discovered && <span style={sx("font:600 8.5px Inter;letter-spacing:.06em;color:var(--dim3);margin-left:7px")}>NEW</span>}
                        </span>
                        <span style={sx("display:block;font:400 11.5px/1.5 Inter;color:var(--dim2);margin-top:3px")}>{p.error || p.desc}</span>
                        {p.chips.length > 0 && (
                          <span style={sx("display:flex;flex-wrap:wrap;gap:4px;margin-top:8px")}>
                            {p.chips.map((c, j) => (
                              <span key={j} style={sx("padding:2px 7px;border-radius:5px;border:1px solid var(--bda2);background:var(--acbtn);font:500 10px JetBrains Mono,monospace;color:var(--ac2)")}>{c}</span>
                            ))}
                          </span>
                        )}
                        {p.feats.length > 0 && (
                          <span style={sx("display:flex;flex-wrap:wrap;gap:4px;margin-top:4px")}>
                            {p.feats.map((c, j) => (
                              <span key={j} style={sx("padding:2px 7px;border-radius:5px;border:1px solid var(--bd3);background:var(--panel2);font:500 10px Inter;color:var(--dim)")}>{c}</span>
                            ))}
                          </span>
                        )}
                        <span style={sx("display:block;font:500 10px JetBrains Mono,monospace;color:var(--dim3);margin-top:7px;word-break:break-all")}>{p.count}</span>
                      </span>
                      <span style={sx(`flex:none;font:600 10px Inter;letter-spacing:.06em;color:${p.error ? "var(--req)" : "var(--ac)"}`)}>{p.error ? "ERROR" : "USE"}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {V.addErr && (
              <div style={sx("margin-top:12px;padding:9px 12px;border-radius:8px;border:1px solid var(--req);background:var(--panel2);font:400 11px/1.5 Inter;color:var(--req)")}>{V.addErr}</div>
            )}

            <div onClick={V.closeAdd} style={sx("margin-top:16px;display:inline-flex;padding:8px 14px;border-radius:8px;border:1px solid var(--bdi);cursor:pointer;font:600 11.5px Inter;color:var(--dim)")}>Cancel</div>
          </div>
        </div>
      )}

      {V.indexOpen && (
        <div style={sx("position:fixed;inset:0;z-index:60;background:var(--ovl);display:flex;align-items:center;justify-content:center;padding:34px")}>
          <div style={sx("width:1000px;max-width:100%;height:100%;display:flex;flex-direction:column;background:var(--panel);border:1px solid var(--bd4);border-radius:14px;overflow:hidden")}>
            <div style={sx("display:flex;align-items:center;gap:12px;padding:16px 20px;border-bottom:1px solid var(--bd2)")}>
              <span style={sx("font:600 14px Inter")}>Attribute index</span>
              <span style={sx("font:500 11.5px JetBrains Mono,monospace;color:var(--dim2)")}>{V.indexCount}</span>
              <input value={V.query} onChange={V.onQuery} placeholder="filter attributes…" style={sx("flex:1;max-width:280px;height:32px;padding:0 10px;background:var(--input);border:1px solid var(--bdi);border-radius:7px;color:var(--tx);font:400 11.5px JetBrains Mono,monospace")} />
              <span style={sx("flex:1")}></span>
              <div onClick={V.closeIndex} style={sx("width:28px;height:28px;border-radius:7px;border:1px solid var(--bdi);display:flex;align-items:center;justify-content:center;cursor:pointer;color:var(--dim2)")}>×</div>
            </div>
            <div style={sx("flex:1;overflow-y:auto;padding:6px 20px 40px")}>
              {V.indexGroups.map((ig, i) => (
                <div key={i} style={sx("padding-top:18px")}>
                  <div style={sx("font:600 11px Inter;letter-spacing:.06em;text-transform:uppercase;color:var(--dim3);margin-bottom:8px")}>{ig.title}</div>
                  <div style={sx("display:flex;flex-direction:column;gap:1px")}>
                    {ig.rows.map((r, j) => (
                      <div key={j} style={sx("display:grid;grid-template-columns:minmax(0,290px) 86px 74px minmax(0,1fr);gap:12px;padding:8px 11px;border-radius:7px;background:var(--panel3)")}>
                        <span style={sx("font:500 11px JetBrains Mono,monospace;color:var(--tx2);word-break:normal;overflow-wrap:anywhere")}>{r.p}</span>
                        <span style={sx("font:500 10.5px JetBrains Mono,monospace;color:var(--dim2)")}>{r.t}</span>
                        <span style={sx(`font:600 9.5px Inter;letter-spacing:.05em;color:${r.reqFg}`)}>{r.req}</span>
                        <span style={sx("min-width:0;overflow-wrap:anywhere;font:400 11px/1.5 Inter;color:var(--dim)")}>{r.i}</span>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {V.showParamModal && (
        <div style={sx("position:fixed;inset:0;z-index:70;background:var(--ovl);display:flex;align-items:center;justify-content:center;padding:24px")}>
          <div style={sx("width:640px;max-width:94vw;max-height:90vh;display:flex;flex-direction:column;background:var(--panel);border:1px solid var(--bd4);border-radius:14px;overflow:hidden;box-shadow:0 24px 60px rgba(0,0,0,.5)")}>
            <div style={sx("display:flex;align-items:center;gap:11px;padding:16px 20px;border-bottom:1px solid var(--bd2);background:var(--panel2);flex:none")}>
              <span style={sx("font:700 14.5px Inter;color:var(--tx);letter-spacing:-.01em")}>🚀 Save / Onboard Pipeline</span>
              <span style={sx("font:600 10.5px Inter;padding:2px 8px;border-radius:6px;background:var(--acfill);color:var(--ac2);border:1px solid var(--acbd)")}>DEV</span>
              <span style={sx("flex:1")}></span>
              <div onClick={V.closeParamModal} style={sx("width:28px;height:28px;border-radius:7px;border:1px solid var(--bdi);display:flex;align-items:center;justify-content:center;cursor:pointer;color:var(--dim2);font:600 14px Inter")}>×</div>
            </div>

            <div style={sx("padding:20px 22px;overflow-y:auto;display:flex;flex-direction:column;gap:18px")}>
              
              <div style={sx("padding:14px 16px;border-radius:11px;background:var(--panel2);border:1px solid var(--acbd);box-shadow:0 2px 10px rgba(0,0,0,.15)")}>
                <div style={sx("display:flex;align-items:center;justify-content:space-between;margin-bottom:10px")}>
                  <div style={sx("font:700 12px Inter;letter-spacing:.04em;text-transform:uppercase;color:var(--ac2);display:flex;align-items:center;gap:6px")}>
                    <span>📂</span>
                    <span>Export Destination &amp; Storage Path</span>
                  </div>
                  <span style={sx("font:500 10.5px Inter;color:var(--dim2)")}>Where the authored spec is saved</span>
                </div>

                <div style={sx("display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-bottom:12px")}>
                  {V.destOptions.filter(d => d.label.indexOf("locally") === -1).map((d, i) => (
                    <div key={i} onClick={d.go} style={sx(`display:flex;gap:9px;align-items:flex-start;padding:9px 11px;border-radius:8px;cursor:pointer;background:${d.bg};border:1px solid ${d.bd};transition:all .15s`)}>
                      <span style={sx(`flex:none;width:12px;height:12px;margin-top:2px;border-radius:50%;border:1.5px solid ${d.ring};background:${d.dot};box-sizing:border-box`)}></span>
                      <span style={sx("flex:1;min-width:0")}>
                        <span style={sx("display:block;font:600 11.5px Inter;color:var(--tx)")}>{d.label}</span>
                        <span style={sx("display:block;font:400 10px/1.4 Inter;color:var(--dim2);margin-top:1px")}>{d.desc}</span>
                      </span>
                    </div>
                  ))}
                </div>

                <div style={sx("margin-bottom:12px")}>
                  <div style={sx("font:600 11px Inter;color:var(--tx);margin-bottom:5px")}>{V.pathLabel}</div>
                  <input
                    value={V.wsPath}
                    onChange={V.onWsPath}
                    placeholder={V.pathPlaceholder}
                    style={sx("width:100%;box-sizing:border-box;height:34px;padding:0 10px;background:var(--input);border:1px solid var(--bdi);border-radius:7px;color:var(--tx);font:400 11px JetBrains Mono,monospace")}
                  />
                </div>

                <div>
                  <div style={sx("font:600 11px Inter;color:var(--tx);margin-bottom:6px")}>Spec File Format</div>
                  <div style={sx("display:flex;gap:8px;align-items:center")}>
                    {[
                      { id: "json", label: "JSON (.json)" },
                      { id: "yaml", label: "YAML (.yaml)" },
                      { id: "both", label: "Both (.json & .yaml)" }
                    ].map(f => {
                      const on = (V.saveFmtChoice || "json") === f.id;
                      return (
                        <div
                          key={f.id}
                          onClick={() => V.onSaveFmtChoice(f.id)}
                          style={sx(`padding:6px 12px;border-radius:7px;cursor:pointer;font:600 11px Inter;transition:all .15s;background:${on ? "var(--acfill)" : "var(--input)"};color:${on ? "var(--ac2)" : "var(--dim2)"};border:1px solid ${on ? "var(--acbd)" : "var(--bdi)"}`)}
                        >
                          {f.label}
                        </div>
                      );
                    })}
                  </div>
                  <div style={sx("font:400 10.5px Inter;color:var(--dim);margin-top:6px;overflow-wrap:anywhere")}>
                    {V.hasTarget ? (
                      <span>
                        Target: <strong style={sx("color:var(--tx2);font-family:JetBrains Mono,monospace")}>
                          {V.saveFmtChoice === "both"
                            ? (V.wsTarget.replace(/\.[a-z]+$/i, ".json") + " & " + V.wsTarget.replace(/\.[a-z]+$/i, ".yaml"))
                            : (V.wsTarget.replace(/\.[a-z]+$/i, "." + (V.saveFmtChoice || "json")))}
                        </strong>
                      </span>
                    ) : ""}
                  </div>
                </div>
              </div>

              <div style={sx("display:grid;grid-template-columns:1fr 1fr;gap:14px")}>
                <div>
                  <div style={sx("font:600 11.5px Inter;color:var(--tx);margin-bottom:6px")}>Target Catalog <span style={sx("color:var(--req)")}>*</span></div>
                  <input
                    value={V.paramCatalog}
                    onChange={V.onParamCatalog}
                    placeholder="metaflow"
                    style={sx("width:100%;box-sizing:border-box;height:34px;padding:0 10px;background:var(--input);border:1px solid var(--bdi);border-radius:7px;color:var(--tx);font:500 12px JetBrains Mono,monospace")}
                  />
                  <div style={sx("font:400 10px Inter;color:var(--dim3);margin-top:4px")}>Target Unity Catalog name</div>
                </div>

                <div>
                  <div style={sx("font:600 11.5px Inter;color:var(--tx);margin-bottom:6px")}>Environment Tier</div>
                  <div style={sx("width:100%;box-sizing:border-box;height:34px;padding:0 12px;background:var(--input);border:1px solid var(--bdi);border-radius:7px;color:var(--tx);font:600 12px JetBrains Mono,monospace;display:flex;align-items:center;justify-content:space-between")}>
                    <span>DEV</span>
                    <span style={sx("font:500 10px Inter;color:var(--dim3)")}>Active Target</span>
                  </div>
                  <div style={sx("font:400 10px Inter;color:var(--dim3);margin-top:4px")}>Deploying to DEV environment</div>
                </div>
              </div>

              <div style={sx("display:grid;grid-template-columns:1fr;gap:14px")}>
                <div>
                  <div style={sx("font:600 11.5px Inter;color:var(--tx);margin-bottom:6px")}>Dataflow Group ID <span style={sx("color:var(--req)")}>*</span></div>
                  <input
                    value={V.paramGroupId}
                    onChange={V.onParamGroupId}
                    placeholder="dfg_name"
                    style={sx("width:100%;box-sizing:border-box;height:34px;padding:0 10px;background:var(--input);border:1px solid var(--bdi);border-radius:7px;color:var(--tx);font:500 12px JetBrains Mono,monospace")}
                  />
                  <div style={sx("font:400 10px Inter;color:var(--dim3);margin-top:4px")}>Unique pipeline identifier in the control plane</div>
                </div>
              </div>

              <div>
                <div style={sx("font:600 11.5px Inter;color:var(--tx);margin-bottom:8px")}>Onboarding Action Mode</div>
                <div style={sx("display:flex;flex-direction:column;gap:8px")}>
                  {[
                    {
                      id: "CREATE",
                      title: "CREATE — Full Pipeline Onboarding",
                      badge: "Standard",
                      desc: "Registers control-table metadata, creates target Delta tables in Unity Catalog, applies tags, and deploys the Lakeflow pipeline DAG."
                    },
                    {
                      id: "UPDATE",
                      title: "UPDATE — Metadata & Flow Evolution",
                      badge: "Evolution",
                      desc: "Modifies existing pipeline definitions, schemas, or transformations without resetting state tables or lineage audit logs."
                    },
                    {
                      id: "VALIDATE_ONLY",
                      title: "VALIDATE_ONLY — Dry-Run Preflight Check",
                      badge: "Dry-Run",
                      desc: "Validates the spec against UC schemas, volume constraints, and control-table rules without making any real modifications."
                    }
                  ].map(opt => {
                    const active = V.paramActionType === opt.id;
                    return (
                      <div
                        key={opt.id}
                        onClick={() => V.onParamActionType({ target: { value: opt.id } })}
                        style={sx(`display:flex;gap:11px;align-items:flex-start;padding:10px 12px;border-radius:8px;cursor:pointer;background:${active ? "var(--sel)" : "var(--input)"};border:1px solid ${active ? "var(--bda)" : "var(--bdi)"};transition:all .15s`)}
                      >
                        <span style={sx(`flex:none;width:13px;height:13px;margin-top:2px;border-radius:50%;border:1.5px solid ${active ? "var(--ac)" : "var(--bd5)"};background:${active ? "var(--ac)" : "transparent"};box-sizing:border-box`)}></span>
                        <div style={sx("flex:1;min-width:0")}>
                          <div style={sx("display:flex;align-items:center;gap:8px")}>
                            <span style={sx(`font:600 11.5px Inter;color:${active ? "var(--tx)" : "var(--tx2)"}`)}>{opt.title}</span>
                            <span style={sx(`font:500 9.5px Inter;padding:1px 6px;border-radius:4px;background:${active ? "var(--acfill)" : "var(--panel3)"};color:${active ? "var(--ac2)" : "var(--dim2)"}`)}>{opt.badge}</span>
                          </div>
                          <div style={sx("font:400 10.5px/1.4 Inter;color:var(--dim2);margin-top:3px")}>{opt.desc}</div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              <div>
                <div style={sx("font:600 11px Inter;color:var(--dim2);margin-bottom:5px")}>Databricks Job ID (Optional override)</div>
                <input
                  value={V.paramJobId}
                  onChange={V.onParamJobId}
                  placeholder="Auto-detected from deployment"
                  style={sx("width:100%;box-sizing:border-box;height:32px;padding:0 10px;background:var(--input);border:1px solid var(--bdi);border-radius:7px;color:var(--tx);font:400 11px JetBrains Mono,monospace")}
                />
                <div style={sx("font:400 9.5px Inter;color:var(--dim3);margin-top:3px")}>Leave empty to use the deployed onboarding job (${"{resources.jobs.onboarding_job.id}"})</div>
              </div>

              {V.promptMsg && (
                <div style={sx(`padding:10px 12px;border-radius:8px;font:500 11px/1.4 Inter;background:${V.promptErr ? "var(--reqfill)" : "var(--acfill)"};color:${V.promptErr ? "var(--req)" : "var(--ac2)"};border:1px solid ${V.promptErr ? "var(--reqbd)" : "var(--acbd)"}`)}>
                  {V.promptMsg}
                </div>
              )}
            </div>

            <div style={sx("display:flex;align-items:center;justify-content:space-between;gap:12px;padding:14px 20px;border-top:1px solid var(--bd2);background:var(--panel2);flex:none")}>
              <div
                onClick={V.doSaveAloneInPrompt}
                style={sx("display:flex;align-items:center;gap:6px;padding:8px 14px;border-radius:8px;border:1px solid var(--bd);background:var(--btn);cursor:pointer;font:600 11.5px Inter;color:var(--tx2)")}
                title="Save the spec file to the selected storage path without executing the Databricks job"
              >
                <span>💾</span>
                <span>Save Spec to Path</span>
              </div>

              <div style={sx("display:flex;align-items:center;gap:10px")}>
                <div onClick={V.closeParamModal} style={sx("padding:8px 14px;border-radius:8px;border:1px solid var(--bd);background:var(--btn);cursor:pointer;font:600 11.5px Inter;color:var(--dim2)")}>
                  Cancel
                </div>
                <div
                  onClick={V.confirmSaveAndRun}
                  style={sx(`display:flex;align-items:center;gap:7px;padding:9px 18px;border-radius:8px;border:1px solid var(--acbd);background:var(--ac);cursor:pointer;font:600 12px Inter;color:#fff;box-shadow:0 2px 10px rgba(0,0,0,.25);opacity:${V.promptSaving ? 0.7 : 1}`)}
                >
                  <span>🚀</span>
                  <span>{V.promptSaving ? "Saving & Triggering…" : "Save & Run Onboarding Job ↗"}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {V.run && (
        <div style={sx("position:fixed;inset:0;z-index:70;background:var(--ovl);display:flex;align-items:center;justify-content:center;padding:40px")}>
          <div style={sx("width:660px;background:var(--panel);border:1px solid var(--bd4);border-radius:14px;overflow:hidden")}>
            <div style={sx("display:flex;align-items:center;gap:11px;padding:16px 20px;border-bottom:1px solid var(--bd2)")}>
              <span style={sx("font:700 14px Inter;color:var(--tx)")}>🚀 Pipeline Onboarding Execution</span>
              <span style={sx("font:500 11px JetBrains Mono,monospace;color:var(--dim2)")}>{V.runId}</span>
              <span style={sx("flex:1")}></span>
              <span style={sx(`font:600 10.5px Inter;letter-spacing:.05em;color:${V.runStateFg}`)}>{V.runState}</span>
              <div onClick={V.closeRun} style={sx("width:26px;height:26px;border-radius:7px;border:1px solid var(--bdi);display:flex;align-items:center;justify-content:center;cursor:pointer;color:var(--dim2)")}>×</div>
            </div>

            {/* Parameter & Context Summary Card */}
            {V.runParams && (
              <div style={sx("display:grid;grid-template-columns:repeat(auto-fit, minmax(130px, 1fr));gap:9px 14px;padding:12px 20px;background:var(--panel2);border-bottom:1px solid var(--bd2);box-sizing:border-box")}>
                <div>
                  <span style={sx("color:var(--dim3);display:block;font:600 9.5px Inter;text-transform:uppercase;letter-spacing:.04em")}>Target Catalog</span>
                  <span style={sx("color:var(--tx);font:600 11.5px JetBrains Mono,monospace")}>{V.runParams.catalog}</span>
                </div>
                <div>
                  <span style={sx("color:var(--dim3);display:block;font:600 9.5px Inter;text-transform:uppercase;letter-spacing:.04em")}>Environment</span>
                  <span style={sx("color:var(--tx);font:600 11.5px JetBrains Mono,monospace")}>{V.runParams.env}</span>
                </div>
                <div>
                  <span style={sx("color:var(--dim3);display:block;font:600 9.5px Inter;text-transform:uppercase;letter-spacing:.04em")}>Dataflow Group</span>
                  <span style={sx("color:var(--tx);font:600 11.5px JetBrains Mono,monospace")}>{V.runParams.dataflow_group_id}</span>
                </div>
                <div>
                  <span style={sx("color:var(--dim3);display:block;font:600 9.5px Inter;text-transform:uppercase;letter-spacing:.04em")}>Action Mode</span>
                  <span style={sx("color:var(--ac2);font:700 11.5px JetBrains Mono,monospace")}>{V.runParams.action_type}</span>
                </div>
                {V.runParams.job_id && (
                  <div>
                    <span style={sx("color:var(--dim3);display:block;font:600 9.5px Inter;text-transform:uppercase;letter-spacing:.04em")}>Databricks Job ID</span>
                    <span style={sx("color:var(--tx);font:600 11.5px JetBrains Mono,monospace")}>{V.runParams.job_id}</span>
                  </div>
                )}
                <div style={sx("grid-column:1/-1;margin-top:2px")}>
                  <span style={sx("color:var(--dim3);display:block;font:600 9.5px Inter;text-transform:uppercase;letter-spacing:.04em")}>Spec Path</span>
                  <span style={sx("color:var(--tx2);font:500 11px JetBrains Mono,monospace;word-break:break-all")}>{V.runParams.spec_path}</span>
                </div>
              </div>
            )}

            <div style={sx("padding:18px 20px 6px")}>
              <div style={sx("height:5px;border-radius:3px;background:var(--bd3);overflow:hidden;margin-bottom:18px")}>
                <div style={sx(`height:100%;width:${V.runPct};background:var(--ac);transition:width .3s`)}></div>
              </div>
              <div style={sx("display:flex;flex-direction:column;gap:4px")}>
                {V.runStages.map((st, i) => (
                  <div key={i} style={sx(`display:flex;align-items:center;gap:12px;padding:9px 12px;border-radius:8px;background:${st.bg};border:1px solid ${st.bg === "transparent" ? "transparent" : "var(--bda)"}`)}>
                    <span style={sx(`flex:none;width:16px;height:16px;border-radius:50%;border:2px solid ${st.ring};border-top-color:${st.top};animation:${st.anim};box-sizing:border-box`)}></span>
                    <span style={sx(`flex:1;font:500 12px Inter;color:${st.fg}`)}>{st.label}</span>
                    <span style={sx("font:500 10.5px JetBrains Mono,monospace;color:var(--dim3)")}>{st.time}</span>
                  </div>
                ))}
              </div>
            </div>
            {V.runFinished && (
              <a href={V.jobRunUrl} target="_blank" rel="noreferrer" style={sx("display:flex;align-items:center;gap:12px;margin:16px 20px 0;padding:13px 15px;border-radius:10px;border:1px solid var(--acbd);background:var(--acfill);text-decoration:none")}>
                <span style={sx("flex:1;min-width:0")}>
                  <span style={sx("display:block;font:700 12.5px Inter;color:var(--ac2)")}>{V.jobRunLabel}</span>
                  <span style={sx("display:block;font:400 11px JetBrains Mono,monospace;color:var(--dim2);margin-top:3px;overflow-wrap:anywhere")}>{V.jobRunSub}</span>
                </span>
              </a>
            )}
            <pre style={sx("margin:14px 20px 20px;padding:12px 13px;max-height:150px;overflow:auto;background:var(--input);border:1px solid var(--bd2);border-radius:9px;font:400 10.5px/1.7 JetBrains Mono,monospace;color:var(--dim2)")}>{V.runLog}</pre>
          </div>
        </div>
      )}
    </div>
  );
}

// One field cell. Widget kinds mirror §6.1 of the build spec.
function Field({ f, compact }) {
  const small = !!compact;
  return (
    <div style={sx(`grid-column:${f.spanCss};min-width:0;margin-left:${f.indent};opacity:${f.op || "1"}`)}>
      <div style={sx(`display:flex;align-items:center;gap:7px;margin-bottom:${small ? "5px" : "6px"}`)}>
        <span style={sx(`flex:1;min-width:0;font:600 ${small ? "11px" : "11.5px"} JetBrains Mono,monospace;color:var(--${small ? "tx3" : "tx2"});white-space:nowrap;overflow:hidden;text-overflow:ellipsis`)}>{f.l}</span>
        <span style={sx(`flex:none;white-space:nowrap;font:600 8.5px Inter;letter-spacing:.06em;color:${f.tagFg}`)}>{f.tag}</span>
        {f.inert && (
          <span title={f.inertReason} style={sx("flex:none;white-space:nowrap;padding:1px 6px;border-radius:4px;border:1px solid var(--bd5);background:var(--panel2);font:600 8.5px Inter;letter-spacing:.05em;color:var(--dim3)")}>N/A</span>
        )}
        <span onClick={f.info} style={sx(`flex:none;width:${small ? "13px" : "14px"};height:${small ? "13px" : "14px"};border-radius:50%;border:1px solid var(--bd5);color:var(--dim2);font:600 ${small ? "8.5px" : "9px"} Inter;display:flex;align-items:center;justify-content:center;cursor:pointer`)}>i</span>
      </div>

      {f.isInput && (
        <input value={f.value} onChange={f.on} placeholder={f.ph} disabled={f.disabled} readOnly={f.disabled} style={sx(`width:100%;box-sizing:border-box;height:${small ? "32px" : "34px"};padding:0 ${small ? "9px" : "10px"};background:var(--${f.disabled ? "panel2" : "input"});border:1px solid var(--bdi);border-radius:${small ? "6px" : "7px"};color:var(--${f.disabled ? "dim3" : "tx"});font:400 ${small ? "11.5px" : "12px"} JetBrains Mono,monospace;cursor:${f.disabled ? "not-allowed" : "auto"}`)} />
      )}
      {f.isSelect && (
        <select value={f.value} onChange={f.on} disabled={f.disabled} style={sx(`width:100%;box-sizing:border-box;height:${small ? "32px" : "34px"};padding:0 ${small ? "7px" : "8px"};background:var(--${f.disabled ? "panel2" : "input"});border:1px solid var(--bdi);border-radius:${small ? "6px" : "7px"};color:var(--${f.disabled ? "dim3" : "tx"});font:400 ${small ? "11.5px" : "12px"} JetBrains Mono,monospace;cursor:${f.disabled ? "not-allowed" : "auto"}`)}>
          {f.opts.map((o, i) => (
            <option key={i} value={o.v}>{o.t}</option>
          ))}
        </select>
      )}
      {f.isBool && (
        <div onClick={f.toggle} style={sx(`display:flex;align-items:center;gap:${small ? "8px" : "9px"};height:${small ? "32px" : "34px"};cursor:${f.disabled ? "not-allowed" : "pointer"}`)}>
          <span style={sx(`flex:none;width:${small ? "32px" : "34px"};height:${small ? "18px" : "19px"};border-radius:${small ? "9px" : "10px"};background:${f.track};position:relative`)}>
            <span style={sx(`position:absolute;top:2px;left:${f.knob};width:${small ? "14px" : "15px"};height:${small ? "14px" : "15px"};border-radius:50%;background:var(--knob)`)}></span>
          </span>
          <span style={sx(`font:500 ${small ? "11px" : "11.5px"} JetBrains Mono,monospace;color:var(--dim)`)}>{f.boolText}</span>
        </div>
      )}
      {f.isSql && (
        <textarea value={f.value} onChange={f.on} placeholder={f.ph} disabled={f.disabled} readOnly={f.disabled} rows={small ? 2 : 4} style={sx(`width:100%;box-sizing:border-box;padding:${small ? "8px 9px" : "9px 10px"};background:var(--input);border:1px solid var(--bdi);border-radius:${small ? "6px" : "7px"};color:var(--tx);font:400 ${small ? "11.5px/1.55" : "12px/1.6"} JetBrains Mono,monospace;resize:vertical`)}></textarea>
      )}

      {f.isKV && (
        <div style={sx(`display:flex;flex-direction:column;gap:${small ? "6px" : "7px"}`)}>
          {f.rows.map((r, i) => (
            <div key={i} style={sx(`display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr) ${small ? "26px" : "28px"};gap:${small ? "6px" : "7px"}`)}>
              <input value={r.k} onChange={r.onK} placeholder="key" style={sx(`width:100%;box-sizing:border-box;height:${small ? "30px" : "32px"};padding:0 ${small ? "8px" : "9px"};background:var(--input);border:1px solid var(--bdi);border-radius:6px;color:var(--tx);font:400 ${small ? "11px" : "11.5px"} JetBrains Mono,monospace`)} />
              <input value={r.v} onChange={r.onV} placeholder="value" style={sx(`width:100%;box-sizing:border-box;height:${small ? "30px" : "32px"};padding:0 ${small ? "8px" : "9px"};background:var(--input);border:1px solid var(--bdi);border-radius:6px;color:var(--tx);font:400 ${small ? "11px" : "11.5px"} JetBrains Mono,monospace`)} />
              <div onClick={r.del} style={sx(`display:${r.delDisplay};height:${small ? "30px" : "32px"};border-radius:6px;border:1px solid var(--bdi);align-items:center;justify-content:center;cursor:pointer;color:var(--dim3);font:500 ${small ? "13px" : "14px"} Inter`)}>×</div>
            </div>
          ))}
          <div onClick={f.addRow} style={sx(`align-self:flex-start;padding:${small ? "5px 10px" : "6px 11px"};border-radius:6px;border:1px dashed var(--bd5);cursor:pointer;font:600 ${small ? "10.5px" : "11px"} Inter;color:var(--ac2)`)}>+ add entry</div>
        </div>
      )}

      {f.isRepeat && (
        <div style={sx("display:flex;flex-direction:column;gap:11px")}>
          {f.items.map((it, i) => (
            <div key={i} style={sx("border:1px solid var(--bd4);border-radius:10px;background:var(--panel3);padding:14px 15px 16px")}>
              <div style={sx("display:flex;align-items:center;gap:9px;margin-bottom:12px")}>
                <span style={sx("font:600 10px JetBrains Mono,monospace;color:var(--dim4)")}>{it.label}</span>
                <span style={sx("flex:1")}></span>
                <span onClick={it.del} style={sx(`display:${it.delDisplay};font:500 11px Inter;color:var(--dim2);cursor:pointer`)}>remove</span>
              </div>
              <div style={sx("display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:14px 16px")}>
                {it.fields.map((g, j) => (
                  <Field key={j} f={g} compact />
                ))}
              </div>
            </div>
          ))}
          <div onClick={f.add} style={sx(`display:${f.addDisplay};align-self:flex-start;padding:7px 12px;border-radius:7px;border:1px dashed var(--bd5);cursor:pointer;font:600 11.5px Inter;color:var(--ac2)`)}>{f.addLabel}</div>
        </div>
      )}

      {f.inert && f.inertReason && (
        <div style={sx(`font:400 ${small ? "10px/1.45" : "10.5px/1.45"} Inter;color:var(--dim2);margin-top:${small ? "4px" : "5px"}`)}>
          Read-only — {f.inertReason}. It will not be written to the spec.
        </div>
      )}
      <div style={sx(`font:400 ${small ? "10px/1.45" : "10.5px/1.45"} Inter;color:var(--dim3);margin-top:${small ? "4px" : "5px"}`)}>{f.hint}</div>
    </div>
  );
}
