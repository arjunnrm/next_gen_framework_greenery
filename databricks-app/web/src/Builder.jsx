import React from "react";
import Shell from "./Shell.jsx";
import { api } from "./api.js";
import {
  flat, F, T, N, S, B, L, Q, KV, REP, SK,
  LABEL_PREFIXES, shortLabel, CDC, isCdc, isAppendish, hasDeleteMarker, TARGET_TYPES, MODES,
  ENC_FIELDS, DEC_FIELDS, ROOT_SECTIONS, OBS_SECTIONS, TARGET_SECTIONS,
  ING_SECTIONS, TRN_SECTIONS, REC_SECTIONS, RECON_DATASET,
  PRESETS, PHASES, filled, STAGES, newFlow, defaultsFor, repeatList, itemLabel, repeatDefaults
} from "./registry.js";

/* eslint-disable */
// Builder flow kind -> the wiki's JSON reference page for that part of the spec.
// Page slugs come from scripts/build_docs_reference.py's flow names.
const DOC_PAGE_BY_KIND = {
  root: "root",
  ing: "ingestion",
  trn: "transformation",
  rec: "reconciliation",
  obs: "observability"
};

// The authoring engine. Everything above the "Databricks integration" marker is
// the reference builder's logic, unchanged. Below it: config loading, the access
// preflight, save-to-Volume / save-to-Workspace, and the real job run.
export default class Builder extends React.Component {
  state = {
    root:{v:{},kvs:{},reps:{}},
    ing:[], trn:[], rec:[],
    kind:"root", idx:0, showInert:false, fmt:"json", info:null, addOpen:false, indexOpen:false, query:"", copied:false, run:null, runStep:-1, runDone:false,
    theme:"dark", showPreview:true, phase:0, tplQuery:"",
    openOpen:false, openErr:"", openBusy:false, openSrc:"volume", openFiles:null, openDir:"",
    openPath:"", openDirs:[], openCheck:null, openChecking:false,
    discovered:[], knowledge:null, tplTab:"builtin",
    saveOpen:false, dest:"local", wsPath:"/Workspace/Shared/flowx/specs",
    volPath:"/Volumes/main/flowx/onboarding_specs/", savedTo:"", cloneFrom:""
  };

  trackHeader(){
    var self=this;
    var apply=function(){
      var h=document.querySelector("header");
      if(!h) return;
      var px=Math.round(h.getBoundingClientRect().height);
      if(px>0) document.documentElement.style.setProperty("--headerH",px+"px");
    };
    apply();
    this._hdrTimer=setInterval(apply,400);
    try{
      var h=document.querySelector("header");
      if(h&&window.ResizeObserver){ this._ro=new ResizeObserver(apply); this._ro.observe(h); }
    }catch(e){}
    this._onResize=apply;
    window.addEventListener("resize",apply);
  }
  applyTheme(t){
    try{ document.documentElement.setAttribute("data-mfl",t); }catch(e){}
  }

  cur(){
    var s=this.state;
    if(s.kind==="root"||s.kind==="obs") return s.root;
    var arr=s[s.kind]||[]; return arr[s.idx]||{v:{},kvs:{},reps:{}};
  }
  read(p){
    var c=this.cur(), val=c.v[p];
    if(val===undefined||val===null||val==="") return defaultsFor()[p];
    return val;
  }
  effective(f){
    var c=this.cur(), v=c.v[f.p];
    if(v!==undefined&&v!==null&&v!=="") return v;
    if(f.dOf) return f.dOf(this.vget());
    if(f.d!==undefined) return f.d;
    return defaultsFor()[f.p];
  }
  vget(){ var self=this; return function(p){ return self.read(p); }; }
  fval(f){
    var c=this.cur(), v=c.v[f.p];
    var dflt=f.dOf?f.dOf(this.vget()):f.d;
    if(v===undefined||v===null||v==="") v=dflt!==undefined?dflt:defaultsFor()[f.p];
    if(v===undefined||v===null||v==="") return f.k==="bool"?false:"";
    return v;
  }
  setVal(p,val){
    var s=this.state, self=this;
    var mut=function(o){ var c=Object.assign({},o); c.v=Object.assign({},c.v); c.v[p]=val; return c; };
    if(s.kind==="root"||s.kind==="obs"){ this.setState({root:mut(s.root)}); return; }
    var arr=(s[s.kind]||[]).slice(); arr[s.idx]=mut(arr[s.idx]||{v:{},kvs:{},reps:{}});
    var patch={}; patch[s.kind]=arr; this.setState(patch);
  }
  writeItem(scope,key,val){
    this.mutate(function(c){
      var list=(c.reps[scope.path]||[]).slice();
      while(list.length<=scope.i) list.push({});
      list[scope.i]=Object.assign({},list[scope.i]);
      list[scope.i][key]=val;
      c.reps[scope.path]=list;
    });
  }

  mutate(fn){
    var s=this.state;
    var target=(s.kind==="root"||s.kind==="obs")?s.root:(s[s.kind]||[])[s.idx];
    var copy=JSON.parse(JSON.stringify({v:{},kvs:{},reps:{}}));
    copy=Object.assign(copy,{v:Object.assign({},target.v),kvs:JSON.parse(JSON.stringify(target.kvs||{})),reps:JSON.parse(JSON.stringify(target.reps||{}))});
    fn(copy);
    if(s.kind==="root"||s.kind==="obs"){ this.setState({root:copy}); return; }
    var arr=(s[s.kind]||[]).slice(); arr[s.idx]=copy;
    var patch={}; patch[s.kind]=arr; this.setState(patch);
  }

  reqStats(kind,fl){
    var self=this, dflt=defaultsFor();
    var v=function(p){ var x=fl.v[p]; return (x===undefined||x===null||x==="")?dflt[p]:x; };
    var tot=0, done=0;
    var secs=(kind==="ing"?ING_SECTIONS():kind==="trn"?TRN_SECTIONS():kind==="rec"?REC_SECTIONS():kind==="obs"?OBS_SECTIONS():ROOT_SECTIONS())
      .filter(function(sec){ return !sec.w||sec.w(v); });
    var byPhase={};
    var scan=function(f,bucket){
      if(f.w&&!f.w(v)) return;
      if(f.k==="repeat"){
        var rlist=repeatList(fl.reps,f).list;
        var rdef=repeatDefaults(f.fields);
        rlist.forEach(function(it){
          var iv=function(p){ var x=it[p]; return (x===undefined||x===null||x==="")?rdef[p]:x; };
          (f.fields||[]).forEach(function(g){
            if(g.w&&!g.w(iv)) return;
            if(!g.req) return;
            bucket.t++; tot++;
            if(filled(iv(g.p))){ bucket.d++; done++; }
          });
        });
        return;
      }
      if(!f.req) return;
      bucket.t++; tot++;
      if(filled(v(f.p))){ bucket.d++; done++; }
    };
    var phaseOf={};
    (PHASES[kind]||[]).forEach(function(p,pi){ p[1].forEach(function(id){ phaseOf[id]=pi; }); });
    secs.forEach(function(sec){
      var pi=phaseOf[sec.id]===undefined?0:phaseOf[sec.id];
      byPhase[pi]=byPhase[pi]||{t:0,d:0};
      (sec.fields||[]).forEach(function(f){ scan(f,byPhase[pi]); });
    });
    if(kind==="ing"||kind==="trn"){
      var pi=phaseOf["cdc"]===undefined?0:phaseOf["cdc"];
      byPhase[pi]=byPhase[pi]||{t:0,d:0};
      byPhase[pi].t++; tot++;
      if(filled(fl.v["target_config.cdc_load_strategy"])){ byPhase[pi].d++; done++; }
      this.cdcFieldDefs().forEach(function(f){ scan(f,byPhase[pi]); });
    }
    return {total:tot,done:done,byPhase:byPhase};
  }

  storeFor(kind){
    if(kind==="root"||kind==="obs") return this.state.root;
    var arr=this.state[kind]||[]; return arr[this.state.idx]||{v:{},kvs:{},reps:{}};
  }

  overallProgress(){
    var self=this, t=0, d=0;
    ["ing","trn","rec"].forEach(function(k){
      (self.state[k]||[]).forEach(function(fl){ var r=self.reqStats(k,fl); t+=r.total; d+=r.done; });
    });
    ["root","obs"].forEach(function(k){
      var r=self.reqStats(k,self.state.root); t+=r.total; d+=r.done;
    });
    return {total:t,done:d,pct:t?Math.round(d/t*100):0};
  }

  sections(){
    var k=this.state.kind;
    if(k==="root") return ROOT_SECTIONS();
    if(k==="obs") return OBS_SECTIONS();
    if(k==="ing") return ING_SECTIONS();
    if(k==="trn") return TRN_SECTIONS();
    return REC_SECTIONS();
  }

  renderField(f,scope){
    var self=this, k=f.k;
    var isBool=k==="bool";
    var raw=scope?((scope.item[f.p]!==undefined&&scope.item[f.p]!=="")?scope.item[f.p]:(f.d!==undefined?f.d:(isBool?false:""))):this.fval(f);
    var val=isBool?(raw===true||raw==="true"):(raw===undefined?"":raw);
    // Reads a sibling value in the same scope — the repeat item when this field is
    // inside one, otherwise the flow. Used by optsOf/cascade below.
    var sib=scope?function(p){ var x=scope.item[p]; return (x===undefined||x===null||x==="")?undefined:x; }:this.vget();
    var writeOne=function(p,nv){ if(scope) self.writeItem(scope,p,nv); else self.setVal(p,nv); };
    var on=function(e){
      if(f.inert) return;
      var nv=e.target.value;
      writeOne(f.p,nv);
      // A cascading select can narrow a dependent field's options; clear the
      // dependent value when the new parent makes it invalid, so the spec can
      // never carry a combination the dropdown would not offer.
      if(f.cascade){
        var upd=f.cascade(nv,sib)||{};
        Object.keys(upd).forEach(function(p){ writeOne(p,upd[p]); });
      }
    };
    var toggle=function(){
      if(f.inert) return;
      var nv=!val;
      if(scope) self.writeItem(scope,f.p,nv);
      else self.setVal(f.p,nv);
    };
    var kvRows=[];
    if(k==="kv"){
      var rows=scope?(((scope.item.__kv||{})[f.p])||[]):((this.cur().kvs||{})[f.p]||[]);
      var kvVirtual=!rows.length;
      if(kvVirtual) rows=[["",""]];
      kvRows=rows.map(function(r,ri){
        var write=function(nk,nv){
          if(scope) self.mutate(function(c){
            var arr=(c.reps[scope.path]||[]).slice();
            while(arr.length<=scope.i) arr.push({});
            var it=Object.assign({},arr[scope.i]); it.__kv=Object.assign({},it.__kv||{});
            var list=(it.__kv[f.p]||[]).slice();
            while(list.length<=ri) list.push(["",""]);
            list[ri]=[nk,nv]; it.__kv[f.p]=list; arr[scope.i]=it; c.reps[scope.path]=arr;
          });
          else self.mutate(function(c){
            var list=(c.kvs[f.p]||[]).slice();
            while(list.length<=ri) list.push(["",""]);
            list[ri]=[nk,nv]; c.kvs[f.p]=list;
          });
        };
        return {k:r[0],v:r[1],delDisplay:kvVirtual?"none":"flex",
          onK:function(e){write(e.target.value,r[1])},
          onV:function(e){write(r[0],e.target.value)},
          del:function(){
            if(scope) self.mutate(function(c){
              var arr=(c.reps[scope.path]||[]).slice();
              if(!arr[scope.i]) return;
              var it=Object.assign({},arr[scope.i]); it.__kv=Object.assign({},it.__kv||{});
              it.__kv[f.p]=(it.__kv[f.p]||[]).filter(function(_,x){return x!==ri});
              arr[scope.i]=it; c.reps[scope.path]=arr;
            });
            else self.mutate(function(c){ c.kvs[f.p]=(c.kvs[f.p]||[]).filter(function(_,x){return x!==ri}); });
          }};
      });
    }
    var items=[];
    if(k==="repeat"){
      var rl=repeatList(this.cur().reps,f), list=rl.list, virtual=rl.virtual;
      items=list.map(function(it,i){
        return {label:itemLabel(f.l,i,virtual||f.single),
          delDisplay:(f.single||virtual)?"none":"inline",
          del:function(){ self.mutate(function(c){ c.reps[f.p]=(c.reps[f.p]||[]).filter(function(_,x){return x!==i}); }); },
          fields:f.fields.filter(function(g){
            if(!g.w) return true;
            var defs=repeatDefaults(f.fields);
            return g.w(function(p){ var x=it[p]; return (x===undefined||x===null||x==="")?defs[p]:x; });
          }).map(function(g){ return self.renderField(g,{path:f.p,i:i,item:it}); })};
      });
    }
    var hint="";
    if(k==="list") hint="comma-separated list";
    if(f.hint) hint=f.hint;
    // An inert field keeps its own hint — the reason it is inert is surfaced
    // separately, as a badge, rather than overwriting the guidance.
    var inertReason=f.inertReason||"";
    var lbl=shortLabel(f.l);
    var wide=(f.span||(lbl.length>24?2:1))>1;
    return {
      l:lbl, spanCss:wide?"1 / -1":"auto", indent:(f.ind?18:0)+"px", op:f.inert?"0.5":"1",
      inert:!!f.inert, disabled:!!f.inert, inertReason:inertReason,
      tag:f.req?"REQUIRED":"", tagFg:f.req?"var(--req)":"transparent",
      isInput:(k==="text"||k==="num"||k==="list"), isSelect:k==="select", isBool:isBool, isSql:k==="sql", isKV:k==="kv", isRepeat:k==="repeat",
      value:isBool?"":String(val), ph:f.ph||"", on:on,
      opts:(f.optsOf?f.optsOf(sib):(f.opts||[])).map(function(o){return {v:o,t:o===""?"— not set —":o}}),
      toggle:toggle, boolText:val?"true":"false", track:val?"var(--actrack)":"var(--bd4)", knob:val?"17px":"2px",
      rows:kvRows, addRow:function(){
        var shown=kvRows.length;
        if(scope) self.mutate(function(c){
          var arr=(c.reps[scope.path]||[]).slice();
          while(arr.length<=scope.i) arr.push({});
          var it=Object.assign({},arr[scope.i]); it.__kv=Object.assign({},it.__kv||{});
          var list=(it.__kv[f.p]||[]).slice();
          while(list.length<shown) list.push(["",""]);
          it.__kv[f.p]=list.concat([["",""]]); arr[scope.i]=it; c.reps[scope.path]=arr;
        });
        else self.mutate(function(c){
          var list=(c.kvs[f.p]||[]).slice();
          while(list.length<shown) list.push(["",""]);
          c.kvs[f.p]=list.concat([["",""]]);
        });
      },
      items:items, addDisplay:(!f.single&&!!f.addLabel)?"inline-flex":"none", addLabel:f.addLabel||"+ add",
      add:function(){
        var shown=items.length;
        self.mutate(function(c){
          var list=(c.reps[f.p]||[]).slice();
          while(list.length<shown) list.push({});
          c.reps[f.p]=list.concat([{}]);
        });
      },
      hint:hint, info:function(){ self.setState({info:{f:f,sec:f.__sec||"",inert:!!f.inert,inertReason:f.inertReason||"",current:isBool?(val?"true":"false"):(val===undefined||val===null||val===""?"":String(val))}}); }
    };
  }

  cdcFieldDefs(){
    var st=function(v){return v("target_config.cdc_load_strategy")};
    return flat([
      L("target_config.primary_keys","target_config.primary_keys",{req:1,w:function(v){return ["SCD1","SCD2","SCD3","FULL_SNAPSHOT_CDC"].indexOf(st(v))>-1},i:"Business key(s) used by apply_changes / apply_changes_from_snapshot. Required by every CDC strategy as of v1.4.0 -- the keyless FULL_SNAPSHOT_CDC variant was withdrawn with the surrogate-key engine."}),
      T("target_config.sequence_by_column","target_config.sequence_by_column",{w:function(v){return isCdc(st(v))},ph:"__framework_ingestion_timestamp_utc",i:"Ordering column for CDC. Falls back to the framework ingestion timestamp — but if capture_technical_metadata is false you must set this explicitly or the pipeline fails."}),
      L("target_config.columns_to_check","target_config.columns_to_check",{w:function(v){return ["SCD1","SCD2","SCD3"].indexOf(st(v))>-1},req:0,i:"Limits which columns trigger a new history version (SCD2) or a current/previous pivot (SCD3). Never include encrypted columns — AES-GCM's random IV causes spurious changes every run."}),
      L("target_config.columns_to_exclude","target_config.columns_to_exclude",{w:function(v){return ["SCD1","SCD2","SCD3"].indexOf(st(v))>-1},i:"Excludes columns from the target schema (except_column_list) and from comparison. Validation error with APPEND, TRUNCATE_AND_LOAD or FULL_SNAPSHOT_CDC."}),
      T("target_config.cdc_operation_column","target_config.cdc_operation_column",{w:function(v){return hasDeleteMarker(st(v))},ph:"op",i:"Column carrying insert/update/delete indicators. Optional regardless of primary_keys. SCD1/SCD2 pass it to apply_changes as apply_as_deletes; FULL_SNAPSHOT_CDC filters flagged rows out of the snapshot so the diff deletes them by absence. A validation error on SCD3, which has no delete path at all."}),
      L("target_config.cdc_operation_mapping.delete_values","cdc_operation_mapping.delete_values",{req:1,ind:1,w:function(v){var c=v("target_config.cdc_operation_column");return hasDeleteMarker(st(v))&&!!(c&&String(c).trim())},ph:"D",i:"Values in cdc_operation_column meaning this row is a delete."}),
      B("target_config.empty_target_if_source_empty","target_config.empty_target_if_source_empty",{w:function(v){return st(v)==="TRUNCATE_AND_LOAD"},i:"WITHDRAWN — this field currently has no runtime effect. It was meant to stop a zero-row TRUNCATE_AND_LOAD source from blanking the target, but preserving the contents makes the target read itself, which Lakeflow rejects as a graph cycle. It still validates so existing specs stay valid; nothing reads it. Enforce the policy outside the graph, with a post-update task comparing the target's row count across updates.",hint:"withdrawn 2026-08-29 — no runtime effect; an empty source blanks the target either way"}),
      B("target_config.generate_hash_columns","target_config.generate_hash_columns",{w:function(v){return isCdc(st(v))},i:"Adds __framework_hash_key (SHA-256 of primary keys) and __framework_hash_value (SHA-256 of comparison columns). Never added for APPEND or TRUNCATE_AND_LOAD."})
    ]);
  }

  buildFlowObject(kind,fl){
    var self=this, out={};
    var setPath=function(o,path,val){
      var parts=path.split("."), n=o;
      for(var i=0;i<parts.length-1;i++){ n[parts[i]]=n[parts[i]]||{}; n=n[parts[i]]; }
      n[parts[parts.length-1]]=val;
    };
    var defs={};
    var walk=function(secs){ secs.forEach(function(s){ (s.fields||[]).forEach(function(f){ defs[f.p]=f; }); }); };
    var secs=kind==="ing"?ING_SECTIONS():kind==="trn"?TRN_SECTIONS():kind==="rec"?REC_SECTIONS():[];
    walk(secs);
    if(kind!=="rec") this.cdcFieldDefs().forEach(function(f){ defs[f.p]=f; });
    if(kind!=="rec") defs["target_config.cdc_load_strategy"]={k:"select",p:"target_config.cdc_load_strategy"};
    var vf=function(p){ var x=(fl.v||{})[p]; if(x===undefined||x===null||x===""){ var d=defs[p]; return d&&d.d!==undefined?d.d:x; } return x; };
    var UI_ONLY={"source_config.explode_mode":1,"target_config.partition_mode":1};
    if(fl.v["source_config.explode_mode"]==="empty") setPath(out,"source_config.explode_columns",[]);
    if(fl.v["target_config.partition_mode"]==="unpartitioned") setPath(out,"target_config.partition_columns",[]);
    Object.keys(fl.v||{}).forEach(function(p){
      if(p.charAt(0)==="@"||UI_ONLY[p]) return;
      if(p==="source_config.explode_columns"&&fl.v["source_config.explode_mode"]!=="named") return;
      if(p==="target_config.partition_columns"&&fl.v["target_config.partition_mode"]!=="named") return;
      var val=fl.v[p], f=defs[p]||{k:"text"};
      if(val===""||val===undefined||val===null) return;
      if(f.w&&!f.w(vf)) return;
      if(f.k==="list"){ var arr=String(val).split(",").map(function(x){return x.trim()}).filter(function(x){return x}); if(!arr.length)return; setPath(out,p,arr); return; }
      if(f.k==="num"){ var n=Number(val); setPath(out,p,isNaN(n)?val:n); return; }
      setPath(out,p,val);
    });
    Object.keys(fl.kvs||{}).forEach(function(p){
      if(p.charAt(0)==="@") return;
      var o={}; (fl.kvs[p]||[]).forEach(function(r){ if(r[0]) o[r[0]]=r[1]; });
      if(Object.keys(o).length) setPath(out,p,o);
    });
    var itemObj=function(it,childDefs){
      var io={};
      // Which children are list widgets — they are edited as a comma-joined string
      // and must be written back out as arrays. Previously only two names were
      // hardcoded here, so match_keys, compare_columns and
      // destination_config.event_log_tables were emitted as strings, producing a
      // spec the framework rejects.
      var listKeys={};
      (childDefs||[]).forEach(function(g){ if(g&&g.k==="list"&&g.p) listKeys[g.p]=1; });
      if(it.target_catalog||it.target_schema||it.target_table){
        it=Object.assign({},it);
        io.type="table";
        io.table=[it.target_catalog,it.target_schema,it.target_table].filter(function(x){return x}).join(".");
        delete it.target_catalog; delete it.target_schema; delete it.target_table;
      }
      Object.keys(it).forEach(function(kk){
        if(kk==="__kv") return;
        var vv=it[kk]; if(vv===""||vv===undefined||vv===null) return;
        if(listKeys[kk]||kk==="delete_values"||kk==="data_standardization_sql"){
          if(Array.isArray(vv)){ setPath(io,kk,vv); return; }
          setPath(io,kk,String(vv).split(",").map(function(x){return x.trim()}).filter(function(x){return x})); return;
        }
        setPath(io,kk,vv);
      });
      Object.keys(it.__kv||{}).forEach(function(kk){ var o={}; (it.__kv[kk]||[]).forEach(function(r){ if(r[0]) o[r[0]]=r[1]; }); if(Object.keys(o).length) setPath(io,kk,o); });
      return io;
    };
    Object.keys(fl.reps||{}).forEach(function(p){
      if(p.charAt(0)==="@") return;
      var repDef=defs[p];
      var list=(fl.reps[p]||[]).map(function(it){ return itemObj(it,(repDef&&repDef.fields)||[]); }).filter(function(o){return Object.keys(o).length});
      if(!list.length) return;
      if(p==="decrypted_columns"){
        var inputs=out.source_inputs||[];
        list.forEach(function(d){
          var nm=d.input_name; delete d.input_name;
          for(var i=0;i<inputs.length;i++) if(inputs[i].input_name===nm){ inputs[i].decrypted_columns=(inputs[i].decrypted_columns||[]).concat([d]); return; }
          inputs.push({input_name:nm,decrypted_columns:[d]});
        });
        out.source_inputs=inputs; return;
      }
      setPath(out,p,list);
    });
    return out;
  }

  // ── Import: canonical framework spec -> Builder state ────────────────────────
  // The exact inverse of spec()/buildFlowObject(), driven by the same registry
  // field definitions so the two stay in step. Anything the registry does not
  // know about is preserved as a flat dotted value rather than dropped, so a
  // round-trip through Open -> Save does not silently lose attributes.

  defsFor(kind){
    var defs={};
    var secs=kind==="ing"?ING_SECTIONS():kind==="trn"?TRN_SECTIONS():kind==="rec"?REC_SECTIONS():kind==="obs"?OBS_SECTIONS():ROOT_SECTIONS();
    secs.forEach(function(s){ (s.fields||[]).forEach(function(f){ if(f.p) defs[f.p]=f; }); });
    if(kind==="ing"||kind==="trn") this.cdcFieldDefs().forEach(function(f){ if(f.p) defs[f.p]=f; });
    return defs;
  }

  // Flatten one nested object into {v, kvs, reps} using the field defs to decide
  // where each node belongs.
  absorb(obj,defs,prefix,store){
    var self=this;
    Object.keys(obj||{}).forEach(function(key){
      var val=obj[key];
      if(val===undefined||val===null) return;
      var path=prefix?prefix+"."+key:key;
      var def=defs[path];
      var isPlainObj=(typeof val==="object"&&!Array.isArray(val));

      if(def&&def.k==="kv"&&isPlainObj){
        store.kvs[path]=Object.keys(val).map(function(k){ return [k,String(val[k])]; });
        return;
      }
      if(def&&def.k==="repeat"&&Array.isArray(val)){
        store.reps[path]=val.map(function(it){ return self.absorbItem(it,def.fields||[]); });
        return;
      }
      if(Array.isArray(val)){
        if(val.length&&typeof val[0]==="object"){
          store.reps[path]=val.map(function(it){ return self.absorbItem(it,(def&&def.fields)||[]); });
        } else {
          // list widget round-trips as a comma-joined string; [] is meaningful
          store.v[path]=val.join(",");
          if(path==="target_config.partition_columns") store.v["target_config.partition_mode"]=val.length?"named":"unpartitioned";
          if(path==="source_config.explode_columns") store.v["source_config.explode_mode"]=val.length?"named":"empty";
        }
        return;
      }
      if(isPlainObj){ self.absorb(val,defs,path,store); return; }
      store.v[path]=(typeof val==="boolean")?val:String(val);
    });
  }

  // One element of a repeat: flat dotted keys plus __kv for its kv children.
  absorbItem(it,childDefs){
    var defs={}; (childDefs||[]).forEach(function(f){ if(f.p) defs[f.p]=f; });
    var out={}, kv={};
    var walk=function(o,prefix){
      Object.keys(o||{}).forEach(function(key){
        var val=o[key]; if(val===undefined||val===null) return;
        var path=prefix?prefix+"."+key:key;
        var def=defs[path];
        var isPlainObj=(typeof val==="object"&&!Array.isArray(val));
        if(def&&def.k==="kv"&&isPlainObj){ kv[path]=Object.keys(val).map(function(k){return [k,String(val[k])]}); return; }
        if(Array.isArray(val)){ out[path]=val.join(","); return; }
        if(isPlainObj){ walk(val,path); return; }
        out[path]=(typeof val==="boolean")?val:String(val);
      });
    };
    walk(it,"");
    // reconciliation datasets serialise as {type:"table", table:"cat.sch.tbl"}
    if(out.table&&String(out.table).indexOf(".")>-1){
      var parts=String(out.table).split(".");
      if(parts.length===3){ out.target_catalog=parts[0]; out.target_schema=parts[1]; out.target_table=parts[2]; delete out.table; }
    }
    if(Object.keys(kv).length) out.__kv=kv;
    return out;
  }

  flowFromCanonical(kind,obj){
    var store={v:{},kvs:{},reps:{}};
    var defs=this.defsFor(kind);
    var src=Object.assign({},obj);

    // source_inputs[].decrypted_columns is edited as one flat list keyed by input_name
    var dec=[];
    if(Array.isArray(src.source_inputs)){
      src.source_inputs=src.source_inputs.map(function(si){
        var copy=Object.assign({},si);
        (copy.decrypted_columns||[]).forEach(function(d){
          dec.push(Object.assign({input_name:copy.input_name},d));
        });
        delete copy.decrypted_columns;
        return copy;
      });
    }
    this.absorb(src,defs,"",store);
    // Attach decrypted_columns only after source_inputs is in place. buildFlowObject
    // folds this list back into out.source_inputs and then writes source_inputs from
    // its own key, so if decrypted_columns were inserted first that later write would
    // overwrite the merge and the decrypted columns would vanish on save.
    if(dec.length){
      var decDef=null;
      (kind==="trn"?TRN_SECTIONS():[]).forEach(function(sec){ (sec.fields||[]).forEach(function(f){ if(f.p==="decrypted_columns") decDef=f; }); });
      var self2=this;
      store.reps.decrypted_columns=dec.map(function(d){ return self2.absorbItem(d,(decDef&&decDef.fields)||[]); });
    }
    // an omitted partition_columns/explode_columns means "absent", not "unpartitioned"
    if(store.v["target_config.partition_mode"]===undefined) store.v["target_config.partition_mode"]="absent";
    if(store.v["source_config.explode_mode"]===undefined&&kind==="ing") store.v["source_config.explode_mode"]="absent";
    return store;
  }

  loadSpec(doc,label){
    var self=this;
    if(!doc||typeof doc!=="object") { this.setState({openErr:"That file did not parse as an object."}); return; }
    var root={v:{},kvs:{},reps:{}};
    if(doc.dataflow_group_id!==undefined) root.v["@dataflow_group_id"]=String(doc.dataflow_group_id);
    if(doc.pipeline_parameters) root.kvs["@pipeline_parameters"]=Object.keys(doc.pipeline_parameters).map(function(k){return [k,String(doc.pipeline_parameters[k])]});
    if(doc.spark_config) root.kvs["@spark_config"]=Object.keys(doc.spark_config).map(function(k){return [k,String(doc.spark_config[k])]});

    var obsDef=null;
    OBS_SECTIONS().forEach(function(sec){ (sec.fields||[]).forEach(function(f){ if(f.p==="@observability") obsDef=f; }); });
    if(Array.isArray(doc.observability)&&doc.observability.length){
      root.reps["@observability"]=doc.observability.map(function(o){ return self.absorbItem(o,(obsDef&&obsDef.fields)||[]); });
      root.v["@observability_enabled"]=true;
    }

    var ing=(doc.ingestion_flows||[]).map(function(f){ return self.flowFromCanonical("ing",f); });
    var trn=(doc.transformation_flows||[]).map(function(f){ return self.flowFromCanonical("trn",f); });
    var rec=(doc.reconciliation_flows||[]).map(function(f){ return self.flowFromCanonical("rec",f); });

    this.setState({
      root:root, ing:ing, trn:trn, rec:rec,
      kind:ing.length?"ing":(trn.length?"trn":(rec.length?"rec":"root")), idx:0, phase:0,
      openOpen:false, openErr:"",
      savedTo:"Opened "+label+" — "+(ing.length+trn.length+rec.length)+" flow(s)", saveErr:false
    });
  }

  // ── Import sources: local file, Workspace, Volume ────────────────────────────

  parseSpecText(text,label){
    var doc=null;
    try { doc=JSON.parse(text); }
    catch(e){
      // Saved specs may be YAML. Rather than ship a YAML parser, ask the server —
      // /api/spec/import already deserialises both formats.
      //
      // /import answers with the server's *internal* SpecDoc shape, which loadSpec
      // cannot read: it wants the canonical document (dataflow_group_id,
      // ingestion_flows, ...). So round it back through /render to get canonical JSON
      // text. The previous version re-ran JSON.parse on the original YAML instead,
      // which throws for the same reason it threw the first time — so every YAML spec
      // opened from a Volume or Workspace failed, and blamed the server for it.
      var self=this;
      this.setState({openBusy:true});
      api.importSpec({content:text,format:"yaml"}).then(function(r){
        return api.render(r.spec,"json");
      }).then(function(out){
        self.setState({openBusy:false});
        self.loadSpec(JSON.parse(out.content),label);
      }).catch(function(er){
        self.setState({openBusy:false,openErr:(er.code||"ERROR")+": "+(er.message||("Could not parse "+label+" as JSON or YAML."))});
      });
      return;
    }
    this.setState({openBusy:false});
    this.loadSpec(doc,label);
  }

  onLocalFile(ev){
    var self=this, file=ev.target.files&&ev.target.files[0];
    if(!file) return;
    var rd=new FileReader();
    rd.onload=function(){ self.parseSpecText(String(rd.result||""),file.name); };
    rd.onerror=function(){ self.setState({openErr:"Could not read "+file.name}); };
    rd.readAsText(file);
    ev.target.value="";
  }

  rootFor(src){
    var roots=this.state.roots||[];
    return roots.filter(function(r){ return r.kind===(src==="workspace"?"workspace":"volume"); })[0]||null;
  }

  // `path` is optional. Empty means "the configured root"; anything else is passed
  // through as the listing prefix, so an operator can type an arbitrary
  // /Volumes/... or /Workspace/... directory. The server still sanitises it and
  // still refuses traversal, so this widens reach without widening trust.
  listSpecs(src,path){
    var self=this, root=this.rootFor(src);
    if(!root){
      this.setState({openSrc:src,openFiles:[],openDirs:[],openErr:"No "+src+" storage root is configured in config/index.json."});
      return;
    }
    var prefix=(path!==undefined&&path!==null)?String(path):(this.state.openSrc===src?(this.state.openPath||""):"");
    this.setState({openSrc:src,openBusy:true,openErr:"",openFiles:null,openDirs:[],openCheck:null,openPath:prefix,openDir:prefix||root.path||""});
    api.list(root.id,prefix).then(function(r){
      var all=r.entries||[];
      self.setState({
        openBusy:false,
        openFiles:all.filter(function(e){ return e.kind!=="dir"; }),
        openDirs:all.filter(function(e){ return e.kind==="dir"; })
      });
    }).catch(function(e){
      self.setState({openBusy:false,openFiles:[],openDirs:[],openErr:(e.code||"ERROR")+": "+e.message});
    });
  }

  // Read + parse + validate a path without loading it into the builder, so a user
  // can find out whether a file is a usable spec before replacing their work.
  checkPath(){
    var self=this, s=this.state, root=this.rootFor(s.openSrc);
    var path=(s.openPath||"").trim();
    if(!root){ this.setState({openErr:"No "+s.openSrc+" storage root is configured."}); return; }
    if(!path){ this.setState({openErr:"Enter a file path to validate."}); return; }
    this.setState({openChecking:true,openErr:"",openCheck:null});
    api.validatePath({root_id:root.id,path:path}).then(function(rep){
      self.setState({openChecking:false,openCheck:rep});
    }).catch(function(e){
      self.setState({openChecking:false,openErr:(e.code||"ERROR")+": "+e.message});
    });
  }

  // Open whatever path is currently typed, rather than a listed entry.
  openTypedPath(){
    var s=this.state, path=(s.openPath||"").trim();
    if(!path){ this.setState({openErr:"Enter a file path to open."}); return; }
    this.openFromStorage({path:path,name:path.split("/").pop()||path});
  }

  openFromStorage(entry){
    var self=this, root=this.rootFor(this.state.openSrc);
    if(!root) return;
    var path=entry.path||entry.name;
    this.setState({openBusy:true,openErr:""});
    api.read(root.id,path).then(function(r){
      self.parseSpecText(r.content||"",entry.name||path);
    }).catch(function(e){
      self.setState({openBusy:false,openErr:(e.code||"ERROR")+": "+e.message});
    });
  }

  useDiscovered(entry){
    var self=this;
    this.setState({addBusy:true,addErr:""});
    api.template(entry.id).then(function(r){
      var body=r.spec||{};
      if(entry.scope==="spec"){
        // A whole-document template replaces the spec, exactly like Open does.
        self.setState({addOpen:false,addBusy:false});
        self.loadSpec(body,entry.label);
        return;
      }
      var kind=entry.scope==="transformation"?"trn":entry.scope==="reconciliation"?"rec":"ing";
      var flow=self.flowFromCanonical(kind,body);
      var arr=(self.state[kind]||[]).concat([flow]);
      var patch={addOpen:false,addBusy:false,kind:kind,idx:arr.length-1,phase:0,tplQuery:""};
      patch[kind]=arr;
      self.setState(patch);
    }).catch(function(e){
      self.setState({addBusy:false,addErr:(e.code||"ERROR")+": "+e.message});
    });
  }

  spec(){
    var self=this, s=this.state, r=s.root, out={};
    if(filled(r.v["@dataflow_group_id"])) out.dataflow_group_id=r.v["@dataflow_group_id"];
    var pp={}; (r.kvs["@pipeline_parameters"]||[]).forEach(function(x){ if(x[0]&&String(x[0]).trim()) pp[String(x[0]).trim()]=x[1]; });
    if(Object.keys(pp).length) out.pipeline_parameters=pp;
    var sc={}; (r.kvs["@spark_config"]||[]).forEach(function(x){ if(x[0]&&String(x[0]).trim()) sc[String(x[0]).trim()]=x[1]; });
    if(Object.keys(sc).length) out.spark_config=sc;
    if(s.ing.length) out.ingestion_flows=s.ing.map(function(f){return self.buildFlowObject("ing",f)});
    if(s.trn.length) out.transformation_flows=s.trn.map(function(f){return self.buildFlowObject("trn",f)});
    if(s.rec.length) out.reconciliation_flows=s.rec.map(function(f){return self.buildFlowObject("rec",f)});
    var obsDefs=null;
    OBS_SECTIONS().forEach(function(sec){ (sec.fields||[]).forEach(function(f){ if(f.p==="@observability") obsDefs=f; }); });
    var obsDefaults=obsDefs?repeatDefaults(obsDefs.fields):{};
    var obs=(r.reps["@observability"]||[]).map(function(raw){
      var it=Object.assign({},obsDefaults,raw);
      var o={};
      var obsListKeys={};
      (obsDefs&&obsDefs.fields||[]).forEach(function(g){ if(g&&g.k==="list"&&g.p) obsListKeys[g.p]=1; });
      Object.keys(it).forEach(function(kk){ if(kk==="__kv")return; var vv=it[kk]; if(vv===""||vv===undefined)return;
        var parts=kk.split("."), n=o; for(var i=0;i<parts.length-1;i++){n[parts[i]]=n[parts[i]]||{};n=n[parts[i]];}
        // list children (e.g. destination_config.event_log_tables) are edited as a
        // comma-joined string and must be emitted as arrays
        if(obsListKeys[kk]){ n[parts[parts.length-1]]=Array.isArray(vv)?vv:String(vv).split(",").map(function(x){return x.trim()}).filter(function(x){return x}); return; }
        n[parts[parts.length-1]]=(kk==="retry.max_attempts"||kk==="retry.backoff_multiplier"||kk==="timeout_ms")?Number(vv):vv; });
      Object.keys(it.__kv||{}).forEach(function(kk){ var m={}; (it.__kv[kk]||[]).forEach(function(x){if(x[0])m[x[0]]=x[1]}); if(Object.keys(m).length){ var parts=kk.split("."),n=o; for(var i=0;i<parts.length-1;i++){n[parts[i]]=n[parts[i]]||{};n=n[parts[i]];} n[parts[parts.length-1]]=m; } });
      return o;
    }).filter(function(o){ return Object.keys(o).length; });
    if(obs.length&&r.v["@observability_enabled"]===true) out.observability=obs;
    return out;
  }

  yaml(o,ind){
    var self=this, pad=new Array((ind||0)+1).join(" ");
    var scalar=function(v){
      if(typeof v==="boolean") return v?"true":"false";
      if(typeof v==="number") return String(v);
      var s=String(v);
      return /^[\w\/\.\-]+$/.test(s)?s:JSON.stringify(s);
    };
    var lines=[];
    Object.keys(o).forEach(function(k){
      var v=o[k];
      if(Array.isArray(v)){
        if(!v.length){ lines.push(pad+k+": []"); return; }
        lines.push(pad+k+":");
        v.forEach(function(x){
          if(x&&typeof x==="object"&&!Array.isArray(x)){
            var sub=self.yaml(x,(ind||0)+4).split("\n");
            lines.push(pad+"  - "+sub[0].replace(/^\s+/,""));
            sub.slice(1).forEach(function(l){ lines.push(l); });
          } else lines.push(pad+"  - "+scalar(x));
        });
      } else if(v&&typeof v==="object"){
        lines.push(pad+k+":");
        lines.push(self.yaml(v,(ind||0)+2));
      } else lines.push(pad+k+": "+scalar(v));
    });
    return lines.filter(function(l){return l.replace(/\s/g,"")!==""}).join("\n");
  }

  workspacePath(){
    var spec=this.spec();
    var base=(this.state.wsPath||"/Workspace/Shared/flowx/specs").replace(/\/+$/,"");
    return base+"/"+(spec.dataflow_group_id||"onboarding")+"."+this.state.fmt;
  }

  download(){
    var spec=this.spec(), fmt=this.state.fmt;
    var body=fmt==="json"?JSON.stringify(spec,null,2):this.yaml(spec,0);
    var blob=new Blob([body],{type:fmt==="json"?"application/json":"text/yaml"});
    var a=document.createElement("a");
    a.href=URL.createObjectURL(blob);
    a.download=(spec.dataflow_group_id||"onboarding")+"."+fmt;
    a.click();
    setTimeout(function(){URL.revokeObjectURL(a.href)},1000);
  }

  tick(){
    var self=this, s=this.state;
    if(!s.run) return;
    if(s.runStep>=STAGES.length-1){ this.setState({runDone:true}); return; }
    this.setState({runStep:s.runStep+1},function(){ setTimeout(function(){ self.tick(); },900); });
  }
  componentWillUnmount(){
    this._dead=true;
    if(this._hdrTimer) clearInterval(this._hdrTimer);
    if(this._ro) try{ this._ro.disconnect(); }catch(e){}
    if(this._onResize) window.removeEventListener("resize",this._onResize);
  }

  renderVals(){
    var self=this, s=this.state;
    var kindLabels={ing:"Ingestion flows",trn:"Transformation flows",rec:"Reconciliation flows",root:"Ingestion flows",obs:"Observability destinations"};
    var singular={ing:"ingestion flow",trn:"transformation flow",rec:"reconciliation flow",obs:"destination"};
    var listKind=s.kind==="root"?"ing":s.kind;
    var v=this.vget();
    var strategy=this.read("target_config.cdc_load_strategy")||"";

    var secs=this.sections().map(function(sec){
      var ok=!sec.w||sec.w(v);
      if(ok) return sec;
      if(!s.showInert) return null;
      return Object.assign({},sec,{sectionInert:true});
    }).filter(function(x){return x});
    var phaseDefs=PHASES[s.kind]||[["All",secs.map(function(x){return x.id})]];
    var visIds={}; secs.forEach(function(x){ visIds[x.id]=true; });
    var phaseIdx=Math.min(s.phase||0,phaseDefs.length-1);
    var phaseOrder=[];
    phaseDefs.forEach(function(p,pi){ if(p[1].some(function(id){return visIds[id]})) phaseOrder.push(pi); });
    if(phaseOrder.indexOf(phaseIdx)<0&&phaseOrder.length) phaseIdx=phaseOrder[0];
    var curIds=(phaseDefs[phaseIdx]||["",[]])[1];
    var ordered=[];
    curIds.forEach(function(id){ secs.forEach(function(x){ if(x.id===id) ordered.push(x); }); });
    secs=ordered;
    var totalFields=0, shownFields=0;
    var sections=secs.map(function(sec,si){
      var fields=(sec.fields||[]).map(function(f){
        var ok=(!f.w||f.w(v))&&!sec.sectionInert;
        totalFields++;
        if(ok) shownFields++;
        if(!ok&&!s.showInert) return null;
        var g=Object.assign({},f,{__sec:sec.title,__doc:sec.doc||""});
        if(!ok){ g.inert=true; g.inertReason=sec.sectionInert?"not applicable to the current selection":(f.reason?("not applicable — "+f.reason):"not applicable to the current selection"); }
        return self.renderField(g);
      }).filter(function(x){return x});
      return {id:sec.id,num:String(si+1).padStart(2,"0"),title:sec.title,sub:sec.sub,
        doc:self.sectionDocUrl(sec,s.kind),docLabel:"docs",
        count:sec.tabs?"":(fields.length+(fields.length===1?" attribute":" attributes")),isTabs:!!sec.tabs&&!sec.sectionInert,
        titleOp:sec.sectionInert?"0.55":"1",
        flex:sec.half?"1 1 400px":"1 1 100%",minw:sec.half?"340px":"0",
        colsCss:(sec.cols===1)?"minmax(0,1fr)":"repeat(auto-fit,minmax(240px,1fr))",fields:fields};
    });

    var cdcDefs=this.cdcFieldDefs();
    var cdcFields=[];
    if(s.kind==="ing"||s.kind==="trn"){
      cdcFields=cdcDefs.map(function(f){
        var ok=!f.w||f.w(v);
        totalFields++; if(ok) shownFields++;
        if(!ok&&!s.showInert) return null;
        var g=Object.assign({},f,{__sec:"Load strategy",__doc:"#8-target-config-shared-by-ingestion--transformation"});
        if(!ok){ g.inert=true; g.inertReason="not applicable to "+(strategy||"the current selection"); }
        return self.renderField(g);
      }).filter(function(x){return x});
    }
    // No strategy selected yet: highlight no tab and describe the choice instead of
    // pretending APPEND is picked — the export carries no cdc_load_strategy either.
    var curCdc=CDC.filter(function(c){return c[0]===strategy})[0]||["","","No load strategy selected yet. Pick a tab above — only that strategy's parameters are shown, and the spec carries no cdc_load_strategy until you do.","nothing selected"];

    var flowList=listKind==="obs"?((s.root.reps["@observability"]||[]).map(function(raw,i){
      var odefs=null;
      OBS_SECTIONS().forEach(function(sec){ (sec.fields||[]).forEach(function(x){ if(x.p==="@observability") odefs=x; }); });
      var d=Object.assign({},odefs?repeatDefaults(odefs.fields):{},raw);
      var reqs=[["id",d.id],["type",d.type]];
      if(d.type==="OTLP_CONSUMER") reqs.push(["endpoint",d["destination_config.endpoint"]]);
      else reqs.push(["volume_path",d["destination_config.volume_path"]]);
      var done=reqs.filter(function(x){return filled(x[1])}).length;
      var pct=Math.round(done/reqs.length*100);
      return {id:d.id||"(unnamed destination)",
        meta:(d.type||"—")+" › "+(d.enabled===true?"enabled":"disabled"),
        pct:pct+"%",pctFg:pct===100?"var(--ok)":"var(--req)",
        bg:"var(--panel2)",bd:"var(--bd3)",fg:"var(--tx2)",
        go:function(){ self.setState({kind:"obs",phase:0}); },
        del:function(e){ e.stopPropagation(); self.mutate(function(c){ c.reps["@observability"]=(c.reps["@observability"]||[]).filter(function(_,x){return x!==i}); }); }};
    })):(s[listKind]||[]).map(function(fl,i){
      var on=(s.kind===listKind&&s.idx===i);
      var idKey=listKind==="ing"?"dataflow_id":listKind==="trn"?"flow_step_id":"reconciliation_id";
      var meta=listKind==="ing"?((fl.v.source_type||"—")+" › "+(fl.v["target_config.cdc_load_strategy"]||"—"))
        :listKind==="trn"?((fl.v.target_type||"—")+" › "+(fl.v["target_config.cdc_load_strategy"]||"—"))
        :((fl.v["source_config.type"]||"table")+" › "+((fl.reps.target_configs||[]).length+" target(s)"));
      var st=self.reqStats(listKind,fl);
      var pct=st.total?Math.round(st.done/st.total*100):100;
      return {id:fl.v[idKey]||"(unnamed)",meta:meta,
        pct:pct+"%",pctFg:pct===100?"var(--ok)":"var(--req)",
        bg:on?"var(--sel)":"var(--panel2)",bd:on?"var(--bda)":"var(--bd3)",fg:on?"var(--ac2)":"var(--tx2)",
        go:function(){ self.setState({kind:listKind,idx:i,phase:0}); },
        del:function(e){ e.stopPropagation(); var arr=(s[listKind]||[]).filter(function(_,x){return x!==i}); var patch={}; patch[listKind]=arr; patch.idx=Math.max(0,Math.min(s.idx,arr.length-1)); self.setState(patch); }};
    });

    var pageTitles={root:"Spec root",obs:"Observability",ing:"Ingestion flow",trn:"Transformation flow",rec:"Reconciliation flow"};
    var curId="";
    if(s.kind==="ing") curId=this.read("dataflow_id")||"";
    if(s.kind==="trn") curId=this.read("flow_step_id")||"";
    if(s.kind==="rec") curId=this.read("reconciliation_id")||"";

    var allDefs=[];
    [["Ingestion flow",ING_SECTIONS()],["Transformation flow",TRN_SECTIONS()],["Reconciliation flow",REC_SECTIONS()],["Spec root",ROOT_SECTIONS()],["Observability",OBS_SECTIONS()]].forEach(function(pair){
      pair[1].forEach(function(sec){
        (sec.fields||[]).forEach(function(f){
          allDefs.push({group:pair[0]+" · "+sec.title,f:f});
          (f.fields||[]).forEach(function(g){ allDefs.push({group:pair[0]+" · "+sec.title+" · "+f.l,f:g}); });
        });
      });
    });
    cdcDefs.forEach(function(f){ allDefs.push({group:"Target config · load strategy",f:f}); });
    allDefs.push({group:"Target config · load strategy",f:{p:"target_config.cdc_load_strategy",k:"select",req:1,i:"Determines how data is loaded or merged into the target."}});
    var q=(s.query||"").toLowerCase();
    var groups={}, gorder=[], icount=0;
    allDefs.forEach(function(d){
      if(q&&d.f.p.toLowerCase().indexOf(q)<0&&(d.f.l||"").toLowerCase().indexOf(q)<0) return;
      if(!groups[d.group]){ groups[d.group]=[]; gorder.push(d.group); }
      icount++;
      groups[d.group].push({p:d.f.l||d.f.p,t:({text:"string",num:"integer",select:"string",bool:"boolean",list:"array",sql:"string",kv:"object",repeat:"array"})[d.f.k]||"string",
        req:d.f.req?"REQUIRED":"optional",reqFg:d.f.req?"var(--req)":"var(--dim4)",i:d.f.i||""});
    });

    var infoF=s.info?s.info.f:null;
    var runStages=STAGES.map(function(st,i){
      var done=s.runStep>i||s.runDone&&s.runStep>=i, active=s.runStep===i&&!s.runDone;
      return {label:st,bg:active?"var(--sel)":"transparent",fg:done?"var(--tx4)":(active?"var(--tx)":"var(--dim3)"),
        ring:done?"var(--ok)":(active?"var(--actrack)":"var(--bd6)"),top:active?"var(--ac)":(done?"var(--ok)":"var(--bd6)"),
        anim:active?"spin .8s linear infinite":"none",time:done?(1.2+i*0.4).toFixed(1)+"s":(active?"running":"queued")};
    });
    var pad2=function(n){ return (n<10?"0":"")+n; };
    var clock=function(minsIn){
      var total=10*60+7+minsIn;
      return "["+pad2(Math.floor(total/60)%24)+":"+pad2(total%60)+"]";
    };
    var logLines=[];
    for(var i=0;i<=Math.min(s.runStep,STAGES.length-1);i++){
      logLines.push(clock(i)+" "+STAGES[i]+(s.runStep>i||s.runDone?" ✓":" …"));
    }
    if(s.runDone){
      var nFlows=s.ing.length+s.trn.length+s.rec.length;
      var grp=s.root.v["@dataflow_group_id"];
      logLines.push(clock(STAGES.length)+" pipeline updated · "+nFlows+" "+(nFlows===1?"flow":"flows")+" onboarded"+(filled(grp)?" under "+grp:""));
    }

    var spec=this.spec();

    return {
      flowTabs:[["ing","Ingestion"],["trn","Transformation"],["rec","Reconciliation"],["obs","Observability"]].map(function(t){
        var on=s.kind===t[0];
        return {label:t[1],count:t[0]==="obs"?(s.root.reps["@observability"]||[]).length:(s[t[0]]||[]).length,bg:on?"var(--acfill)":"transparent",fg:on?"var(--ac2)":"var(--dim)",
          go:function(){ self.setState({kind:t[0],idx:0,phase:0}); }};
      }),
      coverage:shownFields+" / "+totalFields,
      docsHome:this.docsBase(),
      openIndex:function(){self.setState({indexOpen:true})}, closeIndex:function(){self.setState({indexOpen:false,query:""})},
      indexOpen:s.indexOpen, indexCount:icount+" attributes", query:s.query, onQuery:function(e){self.setState({query:e.target.value})},
      indexGroups:gorder.map(function(g){return {title:g,rows:groups[g]}}),
      selectRoot:function(){self.setState({kind:"root",phase:0})}, selectObs:function(){self.setState({kind:"obs",phase:0})},
      rootBg:s.kind==="root"?"var(--sel)":"var(--panel2)", rootBd:s.kind==="root"?"var(--bda)":"var(--bd3)",
      obsBg:s.kind==="obs"?"var(--sel)":"var(--panel2)", obsBd:s.kind==="obs"?"var(--bda)":"var(--bd3)",
      groupId:s.root.v["@dataflow_group_id"]||"(unnamed)", obsCount:(s.root.reps["@observability"]||[]).length,
      flowKindLabel:kindLabels[s.kind], flowKindSingular:singular[listKind], flowList:flowList,
      openAdd:function(){self.setState({addOpen:true})}, closeAdd:function(){self.setState({addOpen:false})}, addOpen:s.addOpen,
      tplQuery:s.tplQuery, onTplQuery:function(e){ self.setState({tplQuery:e.target.value}); },
      presetCount:(function(){
        var q=(s.tplQuery||"").toLowerCase();
        var n=(PRESETS[listKind]||[]).filter(function(p){
          return p[0]!=="blank"&&(!q||(p[1]+" "+p[2]).toLowerCase().indexOf(q)>-1);
        }).length;
        return n+(n===1?" template":" templates");
      })(),
      cloneOptions:(function(){
        var idKey=listKind==="ing"?"dataflow_id":listKind==="trn"?"flow_step_id":"reconciliation_id";
        var opts=(s[listKind]||[]).map(function(fl,i){ return {v:String(i),t:fl.v[idKey]||("(unnamed "+(i+1)+")")}; });
        return opts.length?opts:[{v:"",t:"— nothing to clone yet —"}];
      })(),
      cloneFrom:s.cloneFrom||"0",
      onCloneFrom:function(e){ self.setState({cloneFrom:e.target.value}); },
      cloneOp:(s[listKind]||[]).length?"1":"0.5",
      cloneDesc:(s[listKind]||[]).length?("Copies every attribute from an existing "+singular[listKind]+". Rename its id, then adjust what differs."):("No "+singular[listKind]+" exists yet — create one first, then you can clone it."),
      doClone:function(){
        var src=(s[listKind]||[])[Number(s.cloneFrom||"0")];
        if(!src) return;
        var idKey=listKind==="ing"?"dataflow_id":listKind==="trn"?"flow_step_id":"reconciliation_id";
        var copy=JSON.parse(JSON.stringify(src));
        copy.v[idKey]=(copy.v[idKey]||"flow")+"_copy";
        var arr=(s[listKind]||[]).concat([copy]);
        var patch={addOpen:false,kind:listKind,idx:arr.length-1,phase:0,tplQuery:"",cloneFrom:""}; patch[listKind]=arr; self.setState(patch);
      },
      scratchLabel:"Create from scratch",
      scratchDesc:"An empty "+singular[listKind]+" with every attribute available. Recommended — templates only pre-fill one known-good combination.",
      startScratch:function(){
        var arr=(s[listKind]||[]).concat([newFlow(listKind,"blank")]);
        var patch={addOpen:false,kind:listKind,idx:arr.length-1,phase:0,tplQuery:""}; patch[listKind]=arr; self.setState(patch);
      },
      presets:(PRESETS[listKind]||[]).filter(function(p){
        if(p[0]==="blank") return false;
        var q=(s.tplQuery||"").toLowerCase();
        return !q||(p[1]+" "+p[2]).toLowerCase().indexOf(q)>-1;
      }).map(function(p){
        // Explain the template from what it actually pre-fills, rather than from a
        // hand-written blurb that can drift out of step with the preset body.
        var pv=(p[3]&&p[3].v)||{}, preps=(p[3]&&p[3].reps)||{}, pkvs=(p[3]&&p[3].kvs)||{};
        var chips=[];
        var add=function(label,val){ if(val!==undefined&&val!==null&&val!=="") chips.push(label+": "+val); };
        add("source_type",pv.source_type);
        add("target_type",pv.target_type);
        add("strategy",pv["target_config.cdc_load_strategy"]);
        add("format",pv["source_config.format"]);
        add("sink",pv["target_config.sink_config.format"]);
        var feats=[];
        if(pv["source_config.source_zip_handling.enabled"]===true) feats.push("ZIP extraction");
        if(pv["source_config.source_zip_handling.pre_extraction_decryption.type"]) feats.push("PGP decrypt");
        if((preps["target_config.encrypted_columns"]||[]).length) feats.push("column encryption");
        if((preps["source_inputs.decrypted_columns"]||[]).length) feats.push("column decryption");
        if((preps["dq_config.rules"]||[]).length) feats.push((preps["dq_config.rules"]||[]).length+" DQ rule(s)");
        if(pv["dq_config.quarantine_table"]) feats.push("quarantine");
        if((preps["governance_tags.column_tags"]||[]).length||(preps["governance_tags.table_tags"]||[]).length) feats.push("governance tags");
        if(pv["target_config.generate_hash_columns"]===true) feats.push("hash columns");
        if(pv["source_config.explode_mode"]) feats.push("explode "+pv["source_config.explode_mode"]);
        if(pv["target_config.auto_ttl.expire_in_days"]) feats.push("auto TTL");
        if(pv["source_config.landing_retention_policy.clean_source"]) feats.push("landing retention");
        if((pv["target_config.partition_mode"]||"")==="named") feats.push("partitioned");
        if(pv["target_config.liquid_clustering_columns"]) feats.push("liquid clustering");
        if(pv.transformation_sql) feats.push("SQL transform");
        if((preps["source_inputs"]||[]).length>1) feats.push((preps["source_inputs"]||[]).length+"-way join");
        if(Object.keys(pkvs["target_config.table_properties"]||{}).length) feats.push("table properties");
        var nAttrs=Object.keys(pv).length+Object.keys(pkvs).length+Object.keys(preps).length;
        return {name:p[1],desc:p[2],chips:chips,feats:feats,
          count:nAttrs?(nAttrs+" attributes pre-filled"):"nothing pre-filled",
          go:function(){
            var arr=(s[listKind]||[]).concat([newFlow(listKind,p[0])]);
            var patch={addOpen:false,kind:listKind,idx:arr.length-1,phase:0,tplQuery:""}; patch[listKind]=arr; self.setState(patch);
          }};
      }),
      // Templates discovered on the server, filtered to the kind being added and by
      // the same search box as the built-ins.
      discoveredTemplates:(function(){
        var want=listKind==="trn"?"transformation":listKind==="rec"?"reconciliation":"ingestion";
        var q=(s.tplQuery||"").toLowerCase();
        return (s.discovered||[]).filter(function(t){
          if(t.scope!==want&&t.scope!=="spec") return false;
          if(!q) return true;
          return (t.label+" "+t.description+" "+(t.tags||[]).join(" ")).toLowerCase().indexOf(q)>-1;
        }).map(function(t){
          var cfgChips=Object.keys((t.summary&&t.summary.config)||{}).map(function(k){ return k+": "+t.summary.config[k]; });
          return {
            name:t.label, desc:t.description, file:t.file, scope:t.scope,
            chips:cfgChips, feats:(t.summary&&t.summary.features)||[],
            count:((t.summary&&t.summary.attributes)||0)+" attributes · "+t.file,
            isSpec:t.scope==="spec",
            discovered:t.source!=="index.json",
            error:t.error||"",
            go:function(){ self.useDiscovered(t); }
          };
        });
      })(),
      discoveredCount:(s.discovered||[]).length+" on server",
      addBusy:!!s.addBusy, addErr:s.addErr||"",
      navSections:sections.map(function(x){return {title:x.title,count:x.count,href:"#"+x.id}}),
      phases:(function(){
        var stats=self.reqStats(s.kind,self.storeFor(s.kind));
        return phaseOrder.map(function(pi,n){
          var d=phaseDefs[pi], b=stats.byPhase[pi]||{t:0,d:0};
          var complete=b.t>0&&b.d===b.t, on=pi===phaseIdx;
          return {label:d[0],n:String(n+1),
            bg:on?"var(--sel)":"transparent",bd:on?"var(--bda)":"var(--bd3)",
            fg:on?"var(--tx)":"var(--dim)",
            dot:complete?"var(--ok)":(b.t?"var(--req)":"var(--bd6)"),
            meta:b.t?(b.d+"/"+b.t+" required"):"optional",
            go:function(){ self.setState({phase:pi}); }};
        });
      })(),
      phaseLabel:(phaseDefs[phaseIdx]||["",[]])[0],
      phaseStepText:"Step "+(phaseOrder.indexOf(phaseIdx)+1)+" of "+phaseOrder.length,
      prevPhase:function(){ var n=phaseOrder.indexOf(phaseIdx); if(n>0) self.setState({phase:phaseOrder[n-1]}); },
      nextPhase:function(){ var n=phaseOrder.indexOf(phaseIdx); if(n<phaseOrder.length-1) self.setState({phase:phaseOrder[n+1]}); },
      hasPrev:phaseOrder.indexOf(phaseIdx)>0, hasNext:phaseOrder.indexOf(phaseIdx)<phaseOrder.length-1,
      prevOp:phaseOrder.indexOf(phaseIdx)>0?"1":"0.35", nextOp:phaseOrder.indexOf(phaseIdx)<phaseOrder.length-1?"1":"0.35",
      nextLabel:phaseOrder.indexOf(phaseIdx)<phaseOrder.length-1?("Next · "+(phaseDefs[phaseOrder[phaseOrder.indexOf(phaseIdx)+1]]||["",[]])[0]):"Last step",
      progressPct:self.overallProgress().pct+"%",
      dialogProgress:(function(){
        var o=self.overallProgress();
        var n=(s[listKind]||[]).length;
        return "spec "+o.pct+"% · "+n+" "+(n===1?singular[listKind]:singular[listKind]+"s")+" so far";
      })(),
      progressText:(function(){var o=self.overallProgress();return o.done+"/"+o.total+" required";})(),
      flowProgress:(function(){
        if(s.kind!=="ing"&&s.kind!=="trn"&&s.kind!=="rec") return "";
        if(!(s[s.kind]||[]).length) return "";
        var r=self.reqStats(s.kind,self.cur());
        return r.total?(Math.round(r.done/r.total*100)+"% complete"):"100% complete";
      })(),
      toggleInert:function(){ self.setState({showInert:!self.state.showInert}); },
      inertTrack:s.showInert?"var(--actrack)":"var(--bd4)", inertKnob:s.showInert?"16px":"2px",
      pageTitle:pageTitles[s.kind], pageKind:curId, pageSub:({
        root:"Root-level identity and runtime parameters. Flows live in the three arrays; observability sits alongside them.",
        obs:"Telemetry destinations for the whole spec, configured independently of any flow, plus the columns the framework adds to every target table.",
        ing:"Work through the steps in order. Each selection narrows what follows — source type decides the reader fields, target type decides the sink fields, and the load strategy decides the CDC parameters.",
        trn:"Work through the steps in order. Inputs and SQL come first, then the load strategy decides which CDC parameters apply to the target.",
        rec:"Compare a baseline dataset against one or more target tables, then decide how unmatched records are matched and healed."
      })[s.kind],
      sections:sections,
      cdcTabs:CDC.map(function(c){
        var blocked=(s.kind==="ing"&&c[0]==="SCD3");
        var on=c[0]===strategy;
        return {v:c[0],badge:blocked?"transformation only":c[1],
          bg:on?"var(--panel3)":"transparent",bd:on?"var(--bd4)":"transparent",fg:on?"var(--tx)":"var(--dim2)",
          op:blocked?"0.4":"1",cursor:blocked?"not-allowed":"pointer",
          go:function(){ if(!blocked) self.setVal("target_config.cdc_load_strategy",c[0]); }};
      }),
      cdcCur:curCdc[0], cdcDesc:curCdc[2], cdcParams:curCdc[3], cdcFields:cdcFields,
      themeIcon:s.theme==="dark"?"☾":"☀",
      toggleTheme:function(){
        var t=s.theme==="dark"?"light":"dark";
        self.applyTheme(t);
        try{ window.localStorage.setItem("mfl.theme",t); }catch(e){}
        self.setState({theme:t});
      },
      showPreview:s.showPreview,
      togglePreview:function(){ self.setState({showPreview:!s.showPreview}); },
      previewLabel:s.showPreview?"Hide preview":"Show preview",
      previewBg:s.showPreview?"var(--sel)":"var(--btn)",
      previewFg:s.showPreview?"var(--ac2)":"var(--tx2)",
      gridCols:s.showPreview?"minmax(0,262px) minmax(430px,1fr) minmax(0,380px)":"minmax(0,262px) minmax(0,1fr)",
      fmt:s.fmt, toggleFmt:function(){self.setState({fmt:s.fmt==="json"?"yaml":"json"})},
      setJson:function(){self.setState({fmt:"json"})}, setYaml:function(){self.setState({fmt:"yaml"})},
      jsonBg:s.fmt==="json"?"var(--acfill)":"transparent", jsonFg:s.fmt==="json"?"var(--ac2)":"var(--dim2)",
      yamlBg:s.fmt==="yaml"?"var(--acfill)":"transparent", yamlFg:s.fmt==="yaml"?"var(--ac2)":"var(--dim2)",
      preview:s.fmt==="json"?JSON.stringify(spec,null,2):this.yaml(spec,0),
      // ── Open / upload an existing spec ──────────────────────────────────────
      openOpen:s.openOpen, openBusy:s.openBusy, openErr:s.openErr, openSrc:s.openSrc, openDir:s.openDir,
      showOpen:function(){ self.setState({openOpen:true,openErr:"",openFiles:null}); },
      closeOpen:function(){ self.setState({openOpen:false,openErr:""}); },
      onLocalFile:function(ev){ self.onLocalFile(ev); },
      openSources:[["volume","Unity Catalog Volume"],["workspace","Databricks Workspace"]].map(function(d){
        return {id:d[0],label:d[1],on:s.openSrc===d[0],
          bg:s.openSrc===d[0]?"var(--sel)":"var(--panel3)", bd:s.openSrc===d[0]?"var(--bda)":"var(--bd4)",
          fg:s.openSrc===d[0]?"var(--tx)":"var(--dim)",
          go:function(){ self.listSpecs(d[0]); }};
      }),
      openPath:s.openPath, onOpenPath:function(ev){ self.setState({openPath:ev.target.value,openCheck:null}); },
      openPathPlaceholder:s.openSrc==="workspace"?"/Workspace/Shared/flowx/specs/my_spec.json":"/Volumes/<catalog>/<schema>/<volume>/my_spec.json",
      browseHere:function(){ self.listSpecs(s.openSrc,(s.openPath||"").trim()); },
      openTypedPath:function(){ self.openTypedPath(); },
      checkPath:function(){ self.checkPath(); },
      openChecking:s.openChecking,
      openDirs:(s.openDirs||[]).map(function(e){
        return {name:e.name||e.path, go:function(){ self.listSpecs(s.openSrc,e.path); }};
      }),
      openUp:(function(){
        var cur=(s.openPath||s.openDir||"").replace(/\/+$/,"");
        if(!cur||cur.split("/").length<=3) return null;
        var parent=cur.slice(0,cur.lastIndexOf("/"))||"/";
        return {label:parent, go:function(){ self.listSpecs(s.openSrc,parent); }};
      })(),
      // Validation report for the typed path, if one has been run.
      openCheck:(function(){
        var c=s.openCheck; if(!c) return null;
        return {
          ok:!!c.ok,
          headline:c.ok?"Valid spec":(c.stage==="parse"?"Could not parse this file":"Parsed, but not a valid spec"),
          detail:c.parse_error||"",
          format:c.format||"", bytes:c.bytes||0,
          summary:[
            ["group id",(c.summary&&c.summary.dataflow_group_id)||"—"],
            ["ingestion",String((c.summary&&c.summary.ingestion_flows)||0)],
            ["transformation",String((c.summary&&c.summary.transformation_flows)||0)],
            ["reconciliation",String((c.summary&&c.summary.reconciliation_flows)||0)],
            ["observability",String((c.summary&&c.summary.observability)||0)]
          ],
          errors:(c.errors||[]).slice(0,8).map(function(e){ return (e.field_path?e.field_path+": ":"")+(e.message||""); }),
          more:Math.max(0,(c.errors||[]).length-8),
          warnings:(c.warnings||[]).slice(0,4).map(function(e){ return (e.field_path?e.field_path+": ":"")+(e.message||""); })
        };
      })(),
      openFiles:(s.openFiles||[]).map(function(e){
        var meta=[];
        if(e.size) meta.push(e.size+" bytes");
        if(e.modified){ var d=new Date(Number(e.modified)); if(!isNaN(d.getTime())&&Number(e.modified)>0) meta.push(d.toISOString().slice(0,16).replace("T"," ")); }
        return {name:e.name||e.path, meta:meta.join(" · "),
          go:function(){ self.openFromStorage(e); }};
      }),
      openListed:s.openFiles!==null,
      openEmpty:s.openFiles!==null&&(s.openFiles||[]).length===0,
      saveOpen:s.saveOpen,
      toggleSaveMenu:function(){ self.setState({saveOpen:!self.state.saveOpen,savedTo:""}); },
      isWorkspace:s.dest==="workspace",
      wsPath:s.wsPath, onWsPath:function(e){ self.setState({wsPath:e.target.value}); },
      wsTarget:self.workspacePath(),
      destOptions:[
        ["local","Download locally","Writes the spec to your machine as a ."+s.fmt+" file."],
        ["workspace","Save to workspace path","Writes the spec into a Databricks workspace folder, ready for the onboarding job to pick up."]
      ].map(function(d){
        var on=s.dest===d[0];
        return {label:d[1],desc:d[2],
          bg:on?"var(--sel)":"var(--panel3)",bd:on?"var(--bda)":"var(--bd3)",
          ring:on?"var(--ac)":"var(--bd5)",dot:on?"var(--ac)":"transparent",
          go:function(){ self.setState({dest:d[0],savedTo:""}); }};
      }),
      saveActionLabel:s.dest==="workspace"?("Save to workspace as ."+s.fmt):("Download ."+s.fmt),
      savedTo:s.savedTo,
      save:function(){
        if(s.dest==="workspace"){ self.setState({savedTo:"Saved to "+self.workspacePath()}); return; }
        self.download();
        self.setState({savedTo:"Downloaded "+(spec.dataflow_group_id||"onboarding")+"."+s.fmt});
      },
      copy:function(){
        var body=s.fmt==="json"?JSON.stringify(spec,null,2):self.yaml(spec,0);
        if(navigator.clipboard) navigator.clipboard.writeText(body);
        self.setState({copied:true});
        setTimeout(function(){ self.setState({copied:false}); },1400);
      },
      copyLabel:s.copied?"copied":"copy",
      info:!!s.info, closeInfo:function(){self.setState({info:null})},
      // ── Attribute inspector knowledge overlay ────────────────────────────────
      // config/attribute_knowledge.json, keyed by attribute path. Absent entries are
      // normal: the panel then shows the registry's own description and nothing else,
      // rather than empty scaffolding.
      infoKnow:(function(){
        var kb=s.knowledge; if(!infoF||!kb) return null;
        var entry=(kb.attributes||{})[infoF.p];
        var byWidget=((kb.defaults||{})[infoF.k])||{};
        if(!entry&&!byWidget.tips) return null;
        entry=entry||{};
        var index=kb.reference_index||{};
        return {
          purpose:entry.purpose||"",
          why:entry.why||"",
          samples:(entry.samples||[]).map(function(x){ return {language:x.language||"json",code:x.code||""}; }),
          tips:(entry.tips||[]).concat(byWidget.tips||[]),
          errors:(entry.errors||[]).map(function(e){
            return {symptom:e.symptom||"",cause:e.cause||"",fix:e.fix||""};
          }),
          refs:(entry.refs||[]).map(function(key){
            return {key:key,label:key.split("_").join(" "),url:index[key]||""};
          }).filter(function(r){ return !!r.url; })
        };
      })(),
      infoHasKnow:(function(){
        var kb=s.knowledge; if(!infoF||!kb) return false;
        var e=(kb.attributes||{})[infoF.p];
        var d=((kb.defaults||{})[infoF.k])||{};
        return !!(e||d.tips);
      })(),
      infoName:infoF?(infoF.l||infoF.p):"", infoSection:s.info?s.info.sec:"",
      infoDesc:infoF?(infoF.i||""):"",
      infoPath:infoF?(infoF.p.charAt(0)==="@"?infoF.p.slice(1):infoF.p):"",
      infoHint:infoF?(infoF.hint||""):"",
      infoInert:!!(s.info&&s.info.inert),
      infoInertReason:(s.info&&s.info.inertReason)||"",
      infoCurrent:(s.info&&s.info.current)||"",
      infoCurrentShown:!!(s.info&&s.info.current),
      infoDoc:(infoF&&this.attrDocUrl(infoF.p))||this.flowDocUrl(s.kind),
      infoDocShown:true,
      // Allowed values as individual chips, each with the note the registry carries
      // for it where one exists (CDC strategies are the richest case).
      infoEnum:(function(){
        if(!infoF) return [];
        if(infoF.k==="bool") return [{v:"true",note:""},{v:"false",note:""}];
        var notes={};
        CDC.forEach(function(c){ notes[c[0]]=c[2]||""; });
        return (infoF.opts||[]).map(function(o){
          return {v:o===""?"(unset)":o,note:notes[o]||""};
        });
      })(),
      // Child attributes for object/array-of-object widgets, so the panel explains
      // the whole shape rather than just the container.
      infoChildren:(function(){
        if(!infoF) return [];
        if(infoF.k==="repeat") return (infoF.fields||[]).map(function(g){
          return {p:shortLabel(g.l||g.p),req:!!g.req,i:g.i||""};
        });
        if(infoF.k==="kv") return (infoF.keys||[]).map(function(key){
          return {p:key,req:false,i:""};
        });
        return [];
      })(),
      // Sibling attributes under the same parent object — the usual "what else do
      // I need to set alongside this" question.
      infoRelated:(function(){
        if(!infoF||!infoF.p) return [];
        var path=infoF.p.charAt(0)==="@"?infoF.p.slice(1):infoF.p;
        var i=path.lastIndexOf(".");
        if(i<0) return [];
        var parent=path.slice(0,i);
        var seen={}, out=[];
        (self.sections()||[]).forEach(function(sec){
          (sec.fields||[]).forEach(function(g){
            if(!g.p) return;
            var gp=g.p.charAt(0)==="@"?g.p.slice(1):g.p;
            if(gp===path||seen[gp]) return;
            if(gp.lastIndexOf(".")===parent.length&&gp.indexOf(parent+".")===0){ seen[gp]=1; out.push(gp); }
          });
        });
        return out.slice(0,12);
      })(),
      infoRows:infoF?[
        {k:"type",v:({text:"string",num:"integer",select:"string (enum)",bool:"boolean",list:"array<string>",sql:"string",kv:"object",repeat:"array<object>"})[infoF.k]||"string"},
        {k:"required",v:infoF.req?"yes":"no"},
        {k:"default",v:infoF.d===undefined?"—":String(infoF.d)},
        {k:"sample",v:infoF.ph||"—"},
        {k:"json path",v:infoF.p.charAt(0)==="@"?infoF.p.slice(1):infoF.p}
      ]:[],
      startRun:function(){
        self.setState({run:true,runStep:-1,runDone:false,runNonce:String(Math.floor(Math.random()*900)+100)},function(){ setTimeout(function(){ self.tick(); },350); });
      },
      closeRun:function(){ self.setState({run:null,runStep:-1,runDone:false}); },
      run:!!s.run, runId:"run-"+(s.root.v["@dataflow_group_id"]||"spec"),
      runState:s.runDone?"COMPLETED":"RUNNING", runStateFg:s.runDone?"var(--ok)":"var(--ac)",
      runPct:Math.round(((Math.max(s.runStep,0)+(s.runDone?1:0))/STAGES.length)*100)+"%",
      runStages:runStages, runLog:logLines.join("\n")||"waiting…",
      runFinished:s.runDone,
      jobRunUrl:self._jobRunUrl(s.runNonce||"1"),
      jobRunLabel:"Open job run in Databricks ↗",
      jobRunSub:"Pipeline flowx_"+((s.root.v["@dataflow_group_id"]||"spec").replace(/[^a-z0-9_]+/gi,"_"))+" · run "+(s.runNonce||"1")
    };
  }
  // ───────────────────────────── Databricks integration ─────────────────────────────

  normalizeJobRunUrl(url, runId, jobId){
    var s=this.state, cfg=s.cfg||{};
    var ws=cfg.workspace||{};
    var host = "";
    if (url && typeof url === "string" && (url.startsWith("http://") || url.startsWith("https://"))) {
      try {
        var parsed = new URL(url);
        host = parsed.origin;
      } catch(e) {
        host = ws.host || (typeof window !== "undefined" && window.location ? window.location.origin : "");
      }
    } else {
      host = ws.host || (typeof window !== "undefined" && window.location ? window.location.origin : "");
    }
    host = (host || "").replace(/\/+$/, "");

    var orgId = "";
    if (url && typeof url === "string") {
      var oMatch = url.match(/[?&]o=(\d+)/);
      if (oMatch) orgId = oMatch[1];
    }
    if (!orgId && typeof window !== "undefined" && window.location && window.location.search) {
      try {
        orgId = new URLSearchParams(window.location.search).get("o") || "";
      } catch(e) {}
    }
    if (!orgId && ws.org_id) {
      orgId = String(ws.org_id);
    }

    var effectiveJid = jobId || s.paramJobId || (s.lastRunParams && s.lastRunParams.job_id) || (cfg.actions && cfg.actions.onboard && cfg.actions.onboard.job_id) || cfg.onboarding_job_id || "";
    var effectiveRid = runId || s.runNonce || "1";

    var oParam = orgId ? ("/?o=" + orgId) : "/";

    if (!url || typeof url !== "string" || url.indexOf("jobs//runs") > -1 || url.indexOf("/#job//run") > -1 || url.indexOf("{job_id}") > -1) {
      if (effectiveJid) {
        return host + oParam + "#job/" + effectiveJid + "/run/" + effectiveRid;
      }
      return host + oParam + "#job/run/" + effectiveRid;
    }
    var m = url.match(/\/jobs\/(\d+)?\/runs\/(\d+)/);
    if (m) {
      var j = m[1] || effectiveJid;
      var r = m[2] || effectiveRid;
      if (j) {
        return host + oParam + "#job/" + j + "/run/" + r;
      }
      return host + oParam + "#job/run/" + r;
    }
    if (url.indexOf("#job/") > -1) {
      return url;
    }
    if (effectiveJid) {
      return host + oParam + "#job/" + effectiveJid + "/run/" + effectiveRid;
    }
    return host + oParam + "#job/run/" + effectiveRid;
  }

  _jobRunUrl(runNonce, overrideJobId){
    return this.normalizeJobRunUrl(null, runNonce, overrideJobId);
  }

  componentDidMount(){
    var t=null;
    try{ t=window.localStorage.getItem("mfl.theme"); }catch(e){}
    if(t==="light"||t==="dark") this.setState({theme:t});
    this.applyTheme(t==="light"?"light":"dark");
    this.trackHeader();
    this.loadConfig();
  }

  docsBase(){
    var cfg=this.state.cfg;
    if(cfg&&cfg.docs&&cfg.docs.base_url) return cfg.docs.base_url+(cfg.docs.attribute_reference_page||"");
    return this.props.docsBaseUrl || "/docs/attribute-reference/";
  }

  // ── Wiki deep links ──────────────────────────────────────────────────────────
  // /docs serves the whole MkDocs wiki, so a doc link is a page plus an anchor
  // rather than an anchor on one long page. config.docs_index maps every attribute
  // path to its own heading and is generated by scripts/build_app_docs.py, so these
  // links cannot drift from the wiki. Sections fall back to their flow's reference
  // page, which always exists.
  wikiRoot(){
    var cfg=this.state.cfg;
    return (cfg&&cfg.docs&&cfg.docs.base_url) || "/docs/";
  }
  attrDocUrl(path){
    if(!path) return null;
    var cfg=this.state.cfg, idx=(cfg&&cfg.docs_index)||{};
    var e=idx[path]||idx["@"+path]||idx[String(path).replace(/^@/,"")];
    return e?(this.wikiRoot()+e.page+e.anchor):null;
  }
  flowDocUrl(kind){
    return this.wikiRoot()+"reference/json/"+(DOC_PAGE_BY_KIND[kind]||"root")+"/";
  }
  sectionDocUrl(sec,kind){
    var fs=(sec&&sec.fields)||[];
    for(var i=0;i<fs.length;i++){
      var u=this.attrDocUrl(fs[i].p);
      if(u) return u;
    }
    return this.flowDocUrl(kind);
  }

  loadConfig(){
    var self=this;
    api.config().then(function(cfg){
      var roots=((cfg.app&&cfg.app.spec_storage&&cfg.app.spec_storage.roots)||[]);
      var vol=roots.filter(function(r){return r.kind==="volume"})[0];
      var ws=roots.filter(function(r){return r.kind==="workspace"})[0];
      self.setState({
        cfg:Object.assign({},cfg.app||cfg,{workspace:cfg.workspace||{}}),
        docs:cfg.docs,
        // Templates are discovered server-side from templates/; knowledge backs the
        // attribute inspector. Both arrive with the single config fetch.
        discovered:cfg.templates||[],
        knowledge:(cfg.attribute_knowledge)||((cfg.app||{}).attribute_knowledge)||null,
        roots:roots,
        volPath:(vol&&vol.path ? vol.path.replace("{{catalog}}", (cfg.template_variables&&cfg.template_variables.catalog&&cfg.template_variables.catalog.default)||"flowx") : (self.state.volPath||"/Volumes/flowx/geneva_admin/onboarding_specs/")),
        wsPath:(ws&&ws.path)||self.state.wsPath
      },function(){ self.refreshAccess(); });
    }).catch(function(e){
      self.setState({cfgError:e.message||"config unavailable"});
    });
  }

  currentRoot(){
    var roots=this.state.roots||[];
    var kind=this.state.dest==="volume"?"volume":"workspace";
    return roots.filter(function(r){return r.kind===kind})[0]||null;
  }

  refreshAccess(){
    var self=this, root=this.currentRoot();
    if(!root) { this.setState({access:null}); return; }
    api.access(root.id).then(function(rep){ self.setState({access:rep}); })
      .catch(function(e){ self.setState({access:{checks:[{id:"error",label:e.message,status:"error"}]}}); });
  }

  saveTargetPath(){
    var s=this.state, spec=this.spec();
    var dir=(s.dest==="volume"?(s.volPath||""):(s.wsPath||"")).trim().replace(/\/+$/,"");
    if(!dir) return "";
    var name=(spec.dataflow_group_id||"onboarding")+"."+s.fmt;
    return dir+"/"+name;
  }

  doSave(){
    var self=this, s=this.state, spec=this.spec();
    if(s.dest==="local"){
      this.download();
      this.setState({savedTo:"Downloaded "+(spec.dataflow_group_id||"onboarding")+"."+s.fmt,saveErr:false});
      return;
    }
    var root=this.currentRoot();
    if(!root){ this.setState({savedTo:"No "+s.dest+" root is configured in config/index.json.",saveErr:true}); return; }
    if(!this.saveTargetPath()){ this.setState({savedTo:"Enter a "+(s.dest==="volume"?"Volume directory":"workspace folder")+" first.",saveErr:true}); return; }
    var body=s.fmt==="json"?JSON.stringify(spec,null,2):this.yaml(spec,0);
    this.setState({savedTo:"Writing "+this.saveTargetPath()+" …",saveErr:false});
    api.write({root_id:root.id,path:this.saveTargetPath(),content:body,format:s.fmt,overwrite:true})
      .then(function(r){ self.setState({savedTo:"Saved "+r.path+" · "+r.bytes+" bytes",saveErr:false}); })
      .catch(function(e){
        self.setState({savedTo:(e.code||"ERROR")+": "+e.message+(e.detail?" — "+e.detail:""),saveErr:true});
      });
  }

  openRunPrompt(){
    var s=this.state, spec=this.spec();
    var cfg=s.cfg||{};
    var tv=cfg.template_variables||{};
    var defaultCat=(tv.catalog&&tv.catalog.default)||"flowx";
    var defaultGid=spec.dataflow_group_id||(s.root&&s.root.v&&s.root.v["@dataflow_group_id"])||"dfg_sample";
    var defaultJobId=(cfg.actions&&cfg.actions.onboard&&cfg.actions.onboard.job_id)||cfg.onboarding_job_id||"";
    this.setState({
      showParamModal:true,
      promptSaving:false,
      promptMsg:"",
      promptErr:false,
      saveFmtChoice:s.saveFmtChoice||s.fmt||"json",
      paramCatalog:s.paramCatalog||defaultCat,
      paramEnv:"DEV",
      paramGroupId:defaultGid,
      paramActionType:s.paramActionType||"CREATE",
      paramJobId:(s.paramJobId!==undefined&&s.paramJobId!=="")?s.paramJobId:(defaultJobId?String(defaultJobId):""),
    });
  }

  doSaveAloneInPrompt(){
    var self=this, s=this.state, spec=this.spec();
    var dir=(s.dest==="volume"?(s.volPath||""):(s.wsPath||"")).trim().replace(/\/+$/,"");
    var gid=(spec.dataflow_group_id||"onboarding").trim();
    var fmtChoice=s.saveFmtChoice||s.fmt||"json";

    if(s.dest==="local"){
      this.download();
      this.setState({promptMsg:"Downloaded "+gid+"."+s.fmt+" locally",promptErr:false});
      return;
    }
    var root=this.currentRoot();
    if(!root){ this.setState({promptMsg:"No "+s.dest+" root configured in config/index.json.",promptErr:true}); return; }
    if(!dir){ this.setState({promptMsg:"Enter a target "+(s.dest==="volume"?"Volume directory":"workspace folder")+" first.",promptErr:true}); return; }

    var jsonBody=JSON.stringify(spec,null,2);
    var yamlBody=this.yaml(spec,0);

    if(fmtChoice==="both"){
      var jsonPath=dir+"/"+gid+".json";
      var yamlPath=dir+"/"+gid+".yaml";
      this.setState({promptSaving:true,promptMsg:"Writing "+gid+".json and "+gid+".yaml …",promptErr:false});
      api.write({root_id:root.id,path:jsonPath,content:jsonBody,format:"json",overwrite:true})
        .then(function(rJson){
          api.write({root_id:root.id,path:yamlPath,content:yamlBody,format:"yaml",overwrite:true})
            .then(function(rYaml){
              self.setState({
                promptSaving:false,
                promptMsg:"✅ Saved both "+rJson.path+" and "+rYaml.path,
                promptErr:false,
                savedTo:"Saved "+rJson.path+" & "+rYaml.path,
                saveErr:false
              });
            })
            .catch(function(e){
              self.setState({promptSaving:false,promptMsg:"Saved JSON, but YAML failed: "+(e.code||"ERROR")+": "+e.message,promptErr:true});
            });
        })
        .catch(function(e){
          self.setState({promptSaving:false,promptMsg:(e.code||"ERROR")+": "+e.message+(e.detail?" — "+e.detail:""),promptErr:true});
        });
    } else {
      var singleFmt=fmtChoice==="yaml"?"yaml":"json";
      var singleBody=singleFmt==="yaml"?yamlBody:jsonBody;
      var singlePath=dir+"/"+gid+"."+singleFmt;
      this.setState({promptSaving:true,promptMsg:"Writing spec to "+singlePath+" …",promptErr:false});
      api.write({root_id:root.id,path:singlePath,content:singleBody,format:singleFmt,overwrite:true})
        .then(function(r){
          self.setState({
            promptSaving:false,
            promptMsg:"✅ Spec successfully saved to "+r.path+" ("+r.bytes+" bytes)",
            promptErr:false,
            savedTo:"Saved "+r.path+" · "+r.bytes+" bytes",
            saveErr:false
          });
        })
        .catch(function(e){
          self.setState({
            promptSaving:false,
            promptMsg:(e.code||"ERROR")+": "+e.message+(e.detail?" — "+e.detail:""),
            promptErr:true
          });
        });
    }
  }

  confirmSaveAndRun(){
    var self=this, s=this.state, spec=this.spec();
    var cat=(this.state.paramCatalog||"flowx").trim();
    var env="DEV";
    var gid=(this.state.paramGroupId||"dfg_sample").trim();
    var act=this.state.paramActionType||"CREATE";
    var jid=this.state.paramJobId?parseInt(this.state.paramJobId):undefined;
    var dir=(s.dest==="volume"?(s.volPath||""):(s.wsPath||"")).trim().replace(/\/+$/,"");
    var fmtChoice=s.saveFmtChoice||s.fmt||"json";

    var root=this.currentRoot();
    if(!dir){
      this.setState({promptMsg:"Please provide a valid destination folder.",promptErr:true});
      return;
    }

    var jsonBody=JSON.stringify(spec,null,2);
    var yamlBody=this.yaml(spec,0);
    this.setState({promptSaving:true,promptMsg:"Saving spec before running job…",promptErr:false});

    // Step 1: Save file(s) to selected destination (Volume or Workspace)
    if(fmtChoice==="both"){
      var jsonPath=dir+"/"+gid+".json";
      var yamlPath=dir+"/"+gid+".yaml";
      api.write({root_id:root.id,path:jsonPath,content:jsonBody,format:"json",overwrite:true})
        .then(function(rJson){
          api.write({root_id:root.id,path:yamlPath,content:yamlBody,format:"yaml",overwrite:true})
            .then(function(rYaml){
              self._dispatchOnboardRun(rJson.path, cat, env, gid, act, jid);
            })
            .catch(function(e){
              // If YAML failed but JSON succeeded, proceed with JSON
              self._dispatchOnboardRun(rJson.path, cat, env, gid, act, jid);
            });
        })
        .catch(function(e){
          self.setState({
            promptSaving:false,
            promptMsg:"Save failed before running: "+(e.code||"ERROR")+": "+e.message+(e.detail?" — "+e.detail:""),
            promptErr:true
          });
        });
    } else {
      var singleFmt=fmtChoice==="yaml"?"yaml":"json";
      var singleBody=singleFmt==="yaml"?yamlBody:jsonBody;
      var singlePath=dir+"/"+gid+"."+singleFmt;
      api.write({root_id:root.id,path:singlePath,content:singleBody,format:singleFmt,overwrite:true})
        .then(function(writeRes){
          self._dispatchOnboardRun(writeRes.path, cat, env, gid, act, jid);
        })
        .catch(function(e){
          self.setState({
            promptSaving:false,
            promptMsg:"Save failed before running: "+(e.code||"ERROR")+": "+e.message+(e.detail?" — "+e.detail:""),
            promptErr:true
          });
        });
    }
  }

  _dispatchOnboardRun(savedPath, cat, env, gid, act, jid){
    var self=this;
    var cfg=self.state.cfg||{};
    var effectiveJobId = jid || (cfg.actions&&cfg.actions.onboard&&cfg.actions.onboard.job_id) || cfg.onboarding_job_id || undefined;
    var fileName = savedPath.split("/").pop();

    var runParams = {
      catalog: cat,
      env: env,
      environment: env,
      dataflow_group_id: gid,
      action_type: act,
      spec_path: savedPath,
      spec_file_path: savedPath,
      job_id: effectiveJobId
    };

    var defaultStages = [
      "Upload & stage spec (" + fileName + ")",
      "Validate spec against UC schema & constraints",
      "Upsert control table metadata for " + gid,
      "Register datasets & Delta tables in catalog '" + cat + "'",
      "Apply governance tags & lineage",
      "Onboarding completed successfully"
    ];

    self.setState({
      showParamModal:false,
      promptSaving:false,
      savedTo:"Saved "+savedPath,
      run:true,
      runStep:-1,
      runDone:false,
      lastRunParams: runParams,
      dbx:null,
      dbxErr:"",
      runNonce:String(Math.floor(Math.random()*900)+100)
    },function(){
      // Trigger Databricks onboarding job with OBO client pointing to saved spec path
      var jobParamsToSend = {
        spec_file_path: savedPath,
        catalog: cat,
        env: env,
        action_type: act
      };
      api.runAction("onboard",{
        spec:self.spec(),
        format:self.state.fmt,
        confirmed:true,
        spec_path:savedPath,
        spec_file_path:savedPath,
        params: runParams,
        job_parameters: jobParamsToSend,
        job_id: effectiveJobId
      }).then(function(r){
        self.setState({
          dbx:{
            runId:r.run_id,
            jobId:r.job_id || effectiveJobId,
            url:r.run_url,
            stages:defaultStages,
            current:0,
            state:"RUNNING",
            log:r.log_tail||"",
            done:false,
            startedAt:Date.now()
          }
        });
        self.pollRun(r.run_id);
      }).catch(function(e){
        var errMsg = (e.code||"ERROR")+": "+e.message+(e.detail?(" — "+e.detail):"");
        self.setState({
          dbxErr: errMsg,
          dbx: {
            runId: "FAILED",
            jobId: effectiveJobId,
            url: "",
            stages: defaultStages,
            current: 0,
            state: "FAILED",
            result: "FAILED",
            log: "❌ Databricks Job Trigger Failed:\n" + errMsg,
            done: true,
            startedAt: Date.now()
          }
        });
      });
    });
  }

  startRun(){
    this.openRunPrompt();
  }

  pollRun(runId){
    var self=this;
    if(this._dead) return;
    api.runStatus(runId).then(function(st){
      if(self._dead||!self.state.run) return;
      var terminal=["SUCCESS","FAILED","CANCELED","TIMEDOUT","SKIPPED"].indexOf(st.result||st.state)>-1;
      self.setState({dbx:Object.assign({},self.state.dbx,{
        state:st.state||"RUNNING",result:st.result,current:st.current_stage||0,
        stages:(self.state.dbx&&self.state.dbx.stages)||STAGES,
        log:st.log_tail||"",url:st.run_url||(self.state.dbx&&self.state.dbx.url),done:terminal
      })});
      if(!terminal){
        var iv=(self.state.cfg&&self.state.cfg.actions&&self.state.cfg.actions.onboard&&self.state.cfg.actions.onboard.poll_interval_ms)||3000;
        self._poll=setTimeout(function(){ self.pollRun(runId); },iv);
      }
    }).catch(function(e){
      self.setState({dbxErr:(e.code||"ERROR")+": "+e.message});
    });
  }

  // Overlays the live integration onto the reference view model.
  decorate(V){
    var self=this, s=this.state, roots=s.roots||[];
    var hasVol=roots.some(function(r){return r.kind==="volume"});
    var hasWs=roots.some(function(r){return r.kind==="workspace"});
    var defs=[["local","Download locally","Writes the spec to your machine as a ."+s.fmt+" file."]];
    if(hasVol||!roots.length) defs.push(["volume","Save to Unity Catalog Volume","Writes the spec into a UC Volume as the signed-in user, ready for the onboarding job to read."]);
    if(hasWs||!roots.length) defs.push(["workspace","Save to workspace path","Writes the spec into a Databricks workspace folder."]);
    V.destOptions=defs.map(function(d){
      var on=s.dest===d[0];
      return {label:d[1],desc:d[2],
        bg:on?"var(--sel)":"var(--panel3)",bd:on?"var(--bda)":"var(--bd3)",
        ring:on?"var(--ac)":"var(--bd5)",dot:on?"var(--ac)":"transparent",
        go:function(){ self.setState({dest:d[0],savedTo:""},function(){ self.refreshAccess(); }); }};
    });
    V.isRemote=s.dest!=="local";
    V.pathLabel=s.dest==="volume"?"Volume directory":"Workspace folder";
    V.pathPlaceholder=s.dest==="volume"?"/Volumes/main/flowx/onboarding_specs/":"/Workspace/Shared/flowx/specs";
    V.wsPath=s.dest==="volume"?(s.volPath||""):(s.wsPath||"");
    V.hasTarget=!!this.saveTargetPath();
    V.onWsPath=function(e){
      var val=e.target.value;
      self.setState(s.dest==="volume"?{volPath:val}:{wsPath:val});
    };
    V.wsTarget=this.saveTargetPath();
    V.saveActionLabel=s.dest==="local"?("Download ."+s.fmt):("Save to "+(s.dest==="volume"?"Volume":"workspace")+" as ."+s.fmt);
    V.savedTo=s.savedTo||(s.cfgError?("config: "+s.cfgError):"");
    V.savedToFg=(s.saveErr||s.cfgError)?"var(--req)":"var(--ok)";
    V.save=function(){ self.doSave(); };

    var acc=s.access;
    V.accessRows=(acc&&acc.checks||[]).map(function(c){
      return {label:c.label,remediation:c.remediation||"",
        dot:c.status==="ok"?"var(--ok)":(c.status==="denied"?"var(--req)":"var(--dim3)")};
    });
    V.recheckAccess=function(){ self.refreshAccess(); };

    V.startRun=function(){ self.openRunPrompt(); };
    V.showParamModal=!!s.showParamModal;
    V.closeParamModal=function(){ self.setState({showParamModal:false}); };
    V.saveFmtChoice=s.saveFmtChoice||s.fmt||"json";
    V.onSaveFmtChoice=function(fmt){ self.setState({saveFmtChoice:fmt}); };
    V.paramCatalog=s.paramCatalog||"";
    V.onParamCatalog=function(e){ self.setState({paramCatalog:e.target.value}); };
    V.paramEnv="DEV";
    V.paramGroupId=s.paramGroupId||"";
    V.onParamGroupId=function(e){ self.setState({paramGroupId:e.target.value}); };
    V.paramActionType=s.paramActionType||"CREATE";
    V.onParamActionType=function(e){ self.setState({paramActionType:e.target.value}); };
    V.paramJobId=s.paramJobId||"";
    V.onParamJobId=function(e){ self.setState({paramJobId:e.target.value}); };
    V.promptSaving=!!s.promptSaving;
    V.promptMsg=s.promptMsg||"";
    V.promptErr=!!s.promptErr;
    V.doSaveAloneInPrompt=function(){ self.doSaveAloneInPrompt(); };
    V.confirmSaveAndRun=function(){ self.confirmSaveAndRun(); };
    V.runParams=s.lastRunParams||null;

    var d=s.dbx;
    if(d){
      var stages=d.stages||STAGES;
      var isFailed = d.done && (d.result==="FAILED" || d.state==="FAILED");
      V.runId=d.runId?("run-"+d.runId):V.runId;
      V.runState=d.done?(d.result||d.state||"COMPLETED"):(d.state||"RUNNING");
      V.runStateFg=d.done?((d.result&&d.result!=="SUCCESS")||isFailed?"var(--req)":"var(--ok)"):"var(--ac)";
      V.runStages=stages.map(function(st,i){
        var done=i<d.current||(d.done&&!isFailed), active=i===d.current&&!d.done;
        return {label:st,bg:active?"var(--sel)":(isFailed&&i===0?"var(--reqfill)":"transparent"),
          fg:done?"var(--tx4)":(active?"var(--tx)":(isFailed&&i===0?"var(--req)":"var(--dim3)")),
          ring:done?"var(--ok)":(active?"var(--actrack)":(isFailed&&i===0?"var(--req)":"var(--bd6)")),
          top:active?"var(--ac)":(done?"var(--ok)":(isFailed&&i===0?"var(--req)":"var(--bd6)")),
          anim:active?"spin .8s linear infinite":"none",
          time:done?"done":(active?"running":(isFailed&&i===0?"failed":"queued"))};
      });
      V.runPct=isFailed?"100%":Math.round(((d.done?stages.length:d.current)/Math.max(stages.length,1))*100)+"%";
      V.runLog=(d.log||"")+(s.dbxErr?("\n"+s.dbxErr):"")||"waiting…";
      var resolvedUrl = isFailed ? "" : self.normalizeJobRunUrl(d.url, d.runId, d.jobId || (s.lastRunParams&&s.lastRunParams.job_id));
      V.jobRunUrl=resolvedUrl;
      V.jobRunLabel=isFailed?"Action Failed — Check Error Details in Log":"Open job run "+(d.runId||"")+" in Databricks ↗";
      V.jobRunSub=resolvedUrl;
    } else if(s.dbxErr){
      V.runLog=V.runLog+"\n"+s.dbxErr;
    }
    return V;
  }

  render(){
    var V=this.renderVals();
    this.decorate(V);
    return React.createElement(Shell,{V:V});
  }

}
