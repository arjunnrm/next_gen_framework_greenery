// Metaflow v1.5.0 attribute registry, phases, templates and helpers.
// Lifted verbatim from the approved Spec Builder reference so the 219-attribute
// inventory, every dependency predicate and every template stay byte-identical.
/* eslint-disable */

function flat(a){return a.reduce(function(r,x){return r.concat(Array.isArray(x)?flat(x):[x])},[])}
function F(k,p,l,o){return Object.assign({k:k,p:p,l:l},o||{})}
function T(p,l,o){return F("text",p,l,o)}
function N(p,l,o){return F("num",p,l,o)}
function S(p,l,opts,o){return F("select",p,l,Object.assign({opts:opts},o||{}))}
function B(p,l,o){return F("bool",p,l,o)}
function L(p,l,o){return F("list",p,l,o)}
function Q(p,l,o){return F("sql",p,l,o)}
function KV(p,l,o){return F("kv",p,l,o)}
function REP(p,l,fields,o){return F("repeat",p,l,Object.assign({fields:flat(fields)},o||{}))}
function SK(p,l,req){return [
  T(p+".secret_catalog",l+".secret_catalog",{ind:1,req:req,ph:"{{catalog}}",i:"Unity Catalog secret catalog holding the key."}),
  T(p+".secret_schema",l+".secret_schema",{ind:1,req:req,ph:"security",i:"Unity Catalog secret schema."}),
  T(p+".secret_key",l+".secret_key",{ind:1,req:req,ph:"pii_encryption_key",i:"UC secret key name. AES keys must be exactly 16, 24 or 32 bytes."})
]}

var LABEL_PREFIXES=["source_config.","target_config.","dq_config.","governance_tags.","sink_config.","post_export_archive.","landing_retention_policy.","source_zip_handling.","error_handling.","pgp_encryption.","pre_extraction_decryption."];
function shortLabel(l){
  var out=String(l||""), changed=true;
  while(changed){
    changed=false;
    LABEL_PREFIXES.forEach(function(p){
      if(out.indexOf(p)===0){ out=out.slice(p.length); changed=true; }
    });
  }
  return out;
}

var CDC = [
  ["APPEND","append-only feed","Immutable facts — events, logs, CDRs. No merge, no per-row diff.","no required parameters"],
  ["TRUNCATE_AND_LOAD","full replace","Full extract each run, small enough to recompute entirely.","no required parameters"],
  ["SCD1","overwrite current","Entity state where only the current value matters.","primary_keys required"],
  ["SCD2","full history","Entity state where full history matters.","primary_keys required"],
  ["SCD3","current + previous","Only current and previous value matter. Transformation flows only.","primary_keys + columns_to_check"],
  ["FULL_SNAPSHOT_CDC","snapshot diff","Full extract each run, diffed against the previous one on a declared key to derive inserts, updates and deletes.","primary_keys required"]
];
function isCdc(s){return ["SCD1","SCD2","SCD3","FULL_SNAPSHOT_CDC"].indexOf(s)>-1}
function isAppendish(s){return s==="APPEND"||s==="TRUNCATE_AND_LOAD"}
// Strategies that have a delete path at all, and so accept cdc_operation_column /
// cdc_operation_mapping.delete_values. NOT every CDC strategy: SCD3 is a current/previous
// pivot with no delete semantics, and spec_validator.py's
// `strategies_supporting_delete_marker` rejects the pair outright on it. Offering the field
// there produced a spec the builder accepted and onboarding refused.
function hasDeleteMarker(s){return ["SCD1","SCD2","FULL_SNAPSHOT_CDC"].indexOf(s)>-1}
// Compression codecs each telemetry file format actually supports. SNAPPY is a
// Parquet-internal block codec, so it is offered only for PARQUET; the text
// formats take an external gzip wrapper or nothing.
var VOLUME_COMPRESSION={
  "":["","GZIP","SNAPPY","NONE"],
  "PARQUET":["","SNAPPY","GZIP","NONE"],
  "JSONL":["","GZIP","NONE"],
  "JSON":["","GZIP","NONE"]
};
var TARGET_TYPES=["","streaming_table","materialized_view","batch_table","external_sink","sink"];
var MODES=["","GCM","CBC","ECB"];

var ENC_FIELDS=[
  T("column_name","column_name",{req:1,ph:"pii_column",i:"Plaintext column on this flow's DataFrame to encrypt."}),
  T("output_column","output_column",{ph:"pii_column",i:"Output column name. Defaults to column_name — set the same name to encrypt in place."}),
  S("mode","mode",MODES,{i:"AES cipher mode. GCM is recommended — it adds a random IV."}),
  T("source_data_type","source_data_type",{ph:"string",i:"Optional. The column's original Spark type before encryption replaces it with ciphertext binary (string, decimal(18,2), timestamp, ...). Becomes the Unity Catalog original_data_type tag that a downstream decrypted_columns.cast_to_type is checked against. Leave blank to use the type observed at encryption time; declare it to make a silent source type change fail loudly instead."}),
  SK("secret","secret",1)
];
var DEC_FIELDS=[
  T("input_name","applies to input_name",{req:1,ph:"example_raw_input",i:"Which source_inputs[] entry this decryption belongs to."}),
  T("column_name","column_name",{req:1,ph:"pii_column",i:"Source ciphertext column on the input table."}),
  T("output_column","output_column",{ph:"pii_column_plain",i:"Output plaintext column. Use a different name to keep both ciphertext and plaintext."}),
  S("mode","mode",MODES,{i:"Must match the mode the column was encrypted with."}),
  T("cast_to_type","cast_to_type",{req:1,ph:"string",i:"Spark SQL type to cast the decrypted value to. Cross-validated against the original_data_type tag."}),
  SK("secret","secret",1)
];

function ROOT_SECTIONS(){return [
  {id:"root",title:"Spec root",doc:"#1-top-level-spec-schema",sub:"Root-level fields of the onboarding JSON/YAML. Every control-table row in this spec is upserted under dataflow_group_id.",fields:[
    T("@dataflow_group_id","dataflow_group_id",{req:1,ph:"dfg_finance_txn_ingest",i:"Unique identifier for this pipeline group."}),
    KV("@spark_config","spark_config",{span:2,i:"Group-scoped Spark configuration. Keys must start with \"spark.\"; values may be string, number or boolean. Pipeline and runtime values strictly override the framework's built-in defaults.",hint:"keys must start with spark."}),
    KV("@pipeline_parameters","pipeline_parameters",{span:2,i:"String to value map substituted into ${param} placeholders in transformation_sql, filter_condition and transform_sql. Strings are single-quoted automatically."})
  ]},
  {id:"tmplvars",title:"Template variables",doc:"#20-template-variables",sub:"{{catalog}} and {{env}} resolve before parsing, anywhere in the file. ${param} resolves at execution time, only inside transformation_sql, filter_condition and transform_sql — never wrap it in quotes.",fields:[]}
]}

function OBS_SECTIONS(){return [
  {id:"obsmaster",title:"Observability",doc:"#19-observability",sub:"Telemetry export for the whole spec, configured independently of any flow. Enable it first, then add destinations.",fields:[
    B("@observability_enabled","observability",{span:2,i:"Master switch. When off, the observability array is omitted from the spec entirely and no telemetry is exported.",hint:"turn on to configure destinations"})
  ]},
  {id:"obs",title:"Destinations",doc:"#19-observability",w:function(v){return v("@observability_enabled")===true},sub:"Each destination exports telemetry independently. Does not count toward the at-least-one-flow-array requirement.",fields:[
    REP("@observability","observability[]",[
      T("id","id",{req:1,ph:"dest-otlp-example",i:"Unique destination identifier."}),
      B("enabled","enabled",{i:"Master enable switch for this destination. A disabled destination is dropped entirely by the config loader — callers never see it."}),
      S("type","type",["","DATABRICKS_VOLUME","OTLP_CONSUMER"],{req:1,i:"Where telemetry is exported."}),
      S("mode","mode",["","triggered","continuous"],{i:"triggered extracts the event log once after a pipeline run, bounded by task_id and a start/end time window. continuous streams every configured event-log table together as an always-on process."}),
      L("destination_config.event_log_tables","destination_config.event_log_tables",{req:1,span:2,w:function(v){return v("mode")==="continuous"},ph:"{{catalog}}.silver_example.event_log",i:"Fully-qualified catalog.schema.event_log_table names streamed together in continuous mode.",hint:"continuous mode only"}),
      T("destination_config.volume_path","destination_config.volume_path",{req:1,span:2,w:function(v){return v("type")!=="OTLP_CONSUMER"},ph:"/Volumes/{{catalog}}/observability/app_logs/",i:"Unity Catalog Volume directory telemetry files are written to."}),
      S("destination_config.compression","destination_config.compression",["","GZIP","SNAPPY","NONE"],{w:function(v){return v("type")!=="OTLP_CONSUMER"},
        // Cascades from file_format: SNAPPY is a Parquet block codec and is not a
        // valid wrapper for newline-delimited or plain JSON output.
        optsOf:function(v){ return VOLUME_COMPRESSION[v("destination_config.file_format")||""]||VOLUME_COMPRESSION[""]; },
        i:"Compression applied to exported telemetry files. The choices offered depend on destination_config.file_format — SNAPPY is only valid for PARQUET.",hint:"narrowed by file_format"}),
      S("destination_config.file_format","destination_config.file_format",["","JSONL","JSON","PARQUET"],{w:function(v){return v("type")!=="OTLP_CONSUMER"},
        // Picking a format re-scopes compression; drop an incompatible carry-over.
        cascade:function(nv,v){
          var allowed=VOLUME_COMPRESSION[nv||""]||VOLUME_COMPRESSION[""];
          var cur=v("destination_config.compression");
          return (cur&&allowed.indexOf(cur)<0)?{"destination_config.compression":""}:{};
        },
        i:"On-disk format of exported telemetry files. JSONL is one JSON object per line — the usual choice for telemetry. Selecting a format narrows destination_config.compression to the codecs that format supports.",hint:"drives the compression choices"}),
      T("destination_config.endpoint","destination_config.endpoint",{req:1,span:2,w:function(v){return v("type")==="OTLP_CONSUMER"},ph:"https://otel-collector.internal.net:4318/v1/logs",i:"OTLP collector URL. Volume exports need no endpoint — only a Volume path."}),
      S("destination_config.protocol","destination_config.protocol",["","OTLP_HTTP_JSON","OTLP_HTTP_PROTOBUF","OTLP_GRPC"],{w:function(v){return v("type")==="OTLP_CONSUMER"},i:"Wire protocol used to talk to the collector."}),
      S("destination_config.compression","destination_config.compression",["","gzip","none"],{w:function(v){return v("type")==="OTLP_CONSUMER"},i:"Request compression for OTLP exports."}),
      KV("destination_config.resource_attributes","destination_config.resource_attributes",{span:2,w:function(v){return v("type")==="OTLP_CONSUMER"},i:"OpenTelemetry resource attributes attached to every exported record, e.g. service.name, deployment.environment."}),
      S("auth.type","auth.type",["","BEARER_TOKEN","API_KEY","BASIC_AUTH","NONE"],{w:function(v){return v("type")==="OTLP_CONSUMER"},i:"Authentication scheme. Network destinations only — Volume exports authenticate through Unity Catalog."}),
      KV("auth.credentials","auth.credentials",{w:function(v){return v("type")==="OTLP_CONSUMER"},i:"Credential map. Values must be env:<VAR> or secret:<scope>:<key> references — a literal secret is rejected."}),
      N("retry.max_attempts","retry.max_attempts",{w:function(v){return v("type")==="OTLP_CONSUMER"},ph:"3",i:"How many times a failed network export is retried. Minimum 1. OTLP_CONSUMER only."}),
      N("retry.backoff_multiplier","retry.backoff_multiplier",{w:function(v){return v("type")==="OTLP_CONSUMER"},ph:"2.0",i:"Exponential backoff multiplier between retries. Must be greater than 1. OTLP_CONSUMER only."}),
      N("timeout_ms","timeout_ms",{w:function(v){return v("type")==="OTLP_CONSUMER"},ph:"5000",i:"Request timeout in milliseconds. Not applicable to Volume exports."})
    ],{span:2,addLabel:"+ add destination"})
  ]},
  {id:"fwcols",title:"Framework-generated columns",doc:"#21-framework-generated-columns",sub:"__framework_ingestion_timestamp_utc, __framework_source_file_name/_size/_modification_time/_metadata_headers, __framework_hash_key, __framework_hash_value, __framework_dq_failed_rule_ids, __framework_dq_failure_reasons, __framework_dq_quarantine_flag, __framework_pipeline_run_id, __framework_record_id, __framework_quarantine_validated_at.",fields:[]}
]}

function TARGET_SECTIONS(kind){
  var st=function(v){return v("target_config.cdc_load_strategy")};
  var tt=function(v){return v("target_type")};
  var sinky=function(v){return tt(v)==="sink"||tt(v)==="external_sink"};
  return [
    {id:"cdc",title:"Load strategy",doc:"#8-target-config-shared-by-ingestion--transformation",tabs:1,sub:"target_config.cdc_load_strategy decides how data is merged into the target. Pick a tab — only that strategy's parameters are shown.",fields:[]},
    {id:"storage",title:"Target · storage & table",doc:"#8-target-config-shared-by-ingestion--transformation",sub:"Physical layout of the target table.",fields:flat([
      S("target_config.storage_format","target_config.storage_format",["","delta","iceberg"],{i:"Target table format. iceberg is only valid when target_type is batch_table — for every other target type, leave this on delta and add the table property enable_iceberg_read_uniformity: true instead, which turns on Delta UniForm so Iceberg readers can read the Delta table.",hint:"iceberg: batch_table only · Iceberg reads elsewhere via UniForm table property"}),
      kind==="transformation"?B("target_config.capture_technical_metadata","target_config.capture_technical_metadata",{i:"Transformation flows only. Gates __framework_ingestion_timestamp_utc."}):[],
      S("target_config.partition_mode","partition_columns mode",["","absent","unpartitioned","named"],{w:function(v){return isAppendish(st(v))},i:"absent omits partition_columns entirely. unpartitioned writes partition_columns: [] — an explicit, documented declaration that the table is not partitioned. named writes the columns you list below.",hint:"[] means explicitly unpartitioned, never an error"}),
      L("target_config.partition_columns","target_config.partition_columns",{w:function(v){return isAppendish(st(v))&&v("target_config.partition_mode")==="named"},i:"Physical partitioning. Only effective for APPEND and TRUNCATE_AND_LOAD — silently ignored for CDC-dispatched strategies. Databricks guidance: do not partition tables under 1 TB; use liquid clustering instead.",hint:"partition only above ~1 TB (Databricks guidance)"}),
      L("target_config.liquid_clustering_columns","target_config.liquid_clustering_columns",{w:function(v){return isAppendish(st(v))},i:"Liquid clustering keys. Only effective for APPEND and TRUNCATE_AND_LOAD. Maximum of 3 columns, enforced at onboarding and again at runtime.",hint:"maximum 3 columns"}),
      KV("target_config.table_properties","target_config.table_properties",{span:2,keys:["log_retention_duration","deleted_file_retention_duration","enable_iceberg_read_uniformity"],i:"Delta table properties, added as key-value pairs. Documented keys: log_retention_duration (Delta log retention, e.g. interval 30 days), deleted_file_retention_duration (VACUUM safety window), enable_iceberg_read_uniformity (true enables Delta UniForm so Iceberg readers can read this table — valid for any target_type).",hint:"log_retention_duration · deleted_file_retention_duration · enable_iceberg_read_uniformity"}),
      T("target_config.auto_ttl.timestamp_column","auto_ttl.timestamp_column",{w:function(v){return isAppendish(st(v))},ph:"updated_at",i:"Row age reference column (DATE/TIMESTAMP/TIMESTAMP_NTZ). Only valid for APPEND and TRUNCATE_AND_LOAD — hard error on CDC strategies."}),
      N("target_config.auto_ttl.expire_in_days","auto_ttl.expire_in_days",{w:function(v){return isAppendish(st(v))},ph:"90",i:"Rows older than this many days are auto-deleted."})
    ])},
    {id:"enc",title:"Target · encrypted columns",doc:"#9-encrypted-columns-target_configencrypted_columns",sub:"Each entry encrypts one output column. The framework records the column's pre-encryption Spark type as the original_data_type tag and cross-validates it against cast_to_type when the column is later decrypted. source_data_type pins what that type must be: leave it blank and the observed type is used, fill it in and a source whose type has drifted fails at encryption time instead of breaking the decrypt side later. Decryption never lives in target_config; it belongs to source_inputs[].decrypted_columns.",fields:[
      REP("target_config.encrypted_columns","target_config.encrypted_columns[]",ENC_FIELDS,{span:2,addLabel:"+ encrypt a column"})
    ]},
    {id:"sink",title:"Target · sink config",doc:"#10-sink-config-target_configsink_config",w:sinky,sub:"Required when target_type is sink or external_sink. sink exports only; external_sink writes a governed table and exports.",fields:flat([
      S("target_config.sink_config.format","sink_config.format",["","delta","kafka","pgp_zip"],{req:1,i:"Export format. pgp_zip uses the framework's custom PySpark DataSource."}),
      T("target_config.sink_config.path","sink_config.path",{req:1,w:function(v){var f=v("target_config.sink_config.format");return f!=="kafka"},ph:"/Volumes/{{catalog}}/egress/example/",i:"Output directory. Required for delta and pgp_zip."}),
      S("target_config.sink_config.staged_file_format","sink_config.staged_file_format",["","json","csv"],{w:function(v){return v("target_config.sink_config.format")==="pgp_zip"},i:"File format of the staged export files a pgp_zip sink writes before archiving — json (default when absent, JSON-Lines) or csv (RFC-4180 with header row; one file per written partition). Only meaningful when sink_config.format is pgp_zip."}),
      S("target_config.sink_config.export_trigger","sink_config.export_trigger",["","per_micro_batch","per_update"],{w:function(v){return v("target_config.sink_config.format")==="pgp_zip"},hint:"unset = per_micro_batch",i:"WHAT drives the export. \u0027per_micro_batch\u0027 (the default when absent -- the only pre-v1.7.5 behaviour) feeds the sink from this flow\u0027s own staged view, producing one archive per micro-batch of an append-only stream. \u0027per_update\u0027 decouples the trigger from the payload: an update-scoped pulse drives the sink and the rows are read as a batch, producing exactly ONE export per pipeline update -- including an update that ingested no new rows. \u0027per_update\u0027 is what makes an AGGREGATING target (a materialized_view, or any TRUNCATE_AND_LOAD flow) exportable at all: such a target is fully recomputed each update, which Delta refuses to stream from, so before v1.7.5 it had no sink path whatsoever."}),
      T("target_config.sink_config.staged_file_options.delimiter","staged_file_options.delimiter",{ind:1,w:function(v){return v("target_config.sink_config.format")==="pgp_zip"&&v("target_config.sink_config.staged_file_format")==="csv"},ph:",",i:"Single-character field separator for the staged CSV. Absent = \u0027,\u0027 (Python's csv `excel` dialect, the pre-1.7.4 behaviour). A multi-character delimiter is rejected: Python's csv writer cannot emit one."}),
      B("target_config.sink_config.staged_file_options.include_header","staged_file_options.include_header",{ind:1,w:function(v){return v("target_config.sink_config.format")==="pgp_zip"&&v("target_config.sink_config.staged_file_format")==="csv"},i:"Write the header row. Absent = true. Set false for a supplier interface that specifies a headerless body. NOTE: with archive_format gzip the header is emitted per partition and all but the first are dropped on concatenation."}),
      S("target_config.sink_config.staged_file_options.line_terminator","staged_file_options.line_terminator",["","crlf","lf"],{ind:1,w:function(v){return v("target_config.sink_config.format")==="pgp_zip"&&v("target_config.sink_config.staged_file_format")==="csv"},hint:"unset = crlf",i:"Record separator. Absent = crlf (RFC-4180, the pre-1.7.4 behaviour). Spelled as a name because JSON cannot carry a bare control character."}),
      KV("target_config.sink_config.kafka_options","sink_config.kafka_options",{span:2,req:1,w:function(v){return v("target_config.sink_config.format")==="kafka"},i:"Connection options for a kafka sink — the same flat options a Spark Structured Streaming Kafka writer takes. Onboarding requires both kafka.bootstrap.servers and topic. A kafka sink has no filesystem path. Prefer databricks.serviceCredential over an inline credential. sink_config.kafka_secret_options (option name → UC secret ref, for an option whose literal value must embed a resolved secret such as kafka.sasl.jaas.config) is supported by the framework but cannot be authored here — add it by hand to the exported JSON.",hint:"kafka.bootstrap.servers and topic are both mandatory"}),
      S("target_config.sink_config.write_mode","sink_config.write_mode",["","append","overwrite"],{i:"Delta write mode. Currently accepted but inert — @dlt.append_flow always appends."}),
      B("target_config.sink_config.post_export_archive.enabled","post_export_archive.enabled",{w:function(v){return v("target_config.sink_config.format")==="pgp_zip"},i:"Enable post-write archiving for pgp_zip exports."}),
      T("target_config.sink_config.post_export_archive.output_zip_path","post_export_archive.output_zip_path",{req:1,ind:1,w:function(v){return v("target_config.sink_config.post_export_archive.enabled")===true},ph:"/Volumes/{{catalog}}/egress/zips/",i:"Where the final ZIP is written."}),
      T("target_config.sink_config.post_export_archive.export_file_name_format","post_export_archive.export_file_name_format",{ind:1,w:function(v){return v("target_config.sink_config.post_export_archive.enabled")===true},ph:"export_{batch_id}_{timestamp}.zip",i:"str.format()-style template for the exported archive's own file name. Placeholders: {batch_id}, {timestamp}. Omit for the framework default."}),
      S("target_config.sink_config.post_export_archive.archive_format","post_export_archive.archive_format",["","zip","gzip"],{ind:1,w:function(v){return v("target_config.sink_config.post_export_archive.enabled")===true},hint:"unset = zip",i:"The finished export's container. \u0027zip\u0027 (default when absent) writes an AES-capable ZIP. \u0027gzip\u0027 concatenates this micro-batch's staged partition files into ONE gzip stream \u2014 a gzip holds exactly one member \u2014 named <export_file_name_format>.<csv|jsonl>.gz, or ....gz.gpg when pgp_encryption is on. A gzip stream has no archive password, so post_export_archive.secret is ignored."}),
      SK("target_config.sink_config.post_export_archive.secret","post_export_archive.secret",0).map(function(f){return Object.assign({},f,{reason:"archive_format is gzip \u2014 a gzip stream has no archive password",w:function(v){return v("target_config.sink_config.post_export_archive.enabled")===true&&v("target_config.sink_config.post_export_archive.archive_format")!=="gzip"}})}),
      B("target_config.sink_config.post_export_archive.pgp_encryption.enabled","pgp_encryption.enabled",{ind:1,w:function(v){return v("target_config.sink_config.post_export_archive.enabled")===true},i:"PGP-encrypt the output archive."}),
      SK("target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret","pgp_encryption.recipient_public_key_secret",0).map(function(f){return Object.assign({},f,{reason:"pgp_encryption is off, or passphrase_secret is already set",w:function(v){return v("target_config.sink_config.post_export_archive.pgp_encryption.enabled")===true&&!v("target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_key")}})}),
      SK("target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret","pgp_encryption.passphrase_secret",0).map(function(f){return Object.assign({},f,{reason:"pgp_encryption is off, or recipient_public_key_secret is already set",i:"SYMMETRIC egress encryption \u2014 encrypt with a shared passphrase instead of a recipient key. Mutually exclusive with recipient_public_key_secret: set exactly one. Signing is unavailable in this mode (it needs a sender keypair).",w:function(v){return v("target_config.sink_config.post_export_archive.pgp_encryption.enabled")===true&&!v("target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_key")}})}),
      SK("target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret","pgp_encryption.sign_with_private_key_secret",0).map(function(f){return Object.assign({},f,{w:function(v){return (v("target_config.sink_config.post_export_archive.pgp_encryption.enabled")===true)&&!v("target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_key")}})}),
      SK("target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret","pgp_encryption.sign_passphrase_secret",0).map(function(f){return Object.assign({},f,{reason:"sign_with_private_key_secret is not set",w:function(v){return (!!v("target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_key"))&&!v("target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_key")}})})
    ])},
    {id:"dq",title:"Data quality",doc:"#11-dq-config",sub:"Rules become pipeline expectations. quarantine is a framework extension that routes rows to a sibling table.",fields:[
      REP("dq_config.rules","dq_config.rules[]",[
        T("rule_id","rule_id",{req:1,ph:"dq_amount_non_negative",i:"Unique rule identifier."}),
        S("action","action",["","warn","drop","fail","quarantine"],{req:1,i:"warn logs and keeps the row, drop silently removes it, fail aborts the pipeline, quarantine routes it to the quarantine table."}),
        Q("expression","expression",{req:1,span:2,ph:"amount >= 0",i:"Boolean Spark SQL expression evaluated per row."})
      ],{span:2,addLabel:"+ add rule"}),
      T("dq_config.quarantine_table","dq_config.quarantine_table",{ph:"<target_table>_quarantine",i:"Quarantine table name. Only created when at least one rule uses action quarantine."}),
      T("dq_config.record_id_column","dq_config.record_id_column",{ph:"example_id",i:"Surfaced as __framework_record_id on quarantined rows for traceability."})
    ]},
    {id:"gov",title:"Governance tags",doc:"#12-governance-tags",sub:"Applied post-deployment via ALTER TABLE SET TAGS. This framework applies tags only — it does not create masking or row-filter policies.",fields:[
      REP("governance_tags.column_tags","governance_tags.column_tags[]",[
        T("column","column",{req:1,ph:"pii_column",i:"Column to tag."}),
        KV("tags","tags",{req:1,i:"Key-value tags, e.g. mask: PII, classification: restricted."})
      ],{span:2,addLabel:"+ tag a column"}),
      KV("governance_tags.table_tags","governance_tags.table_tags",{span:2,i:"Table-level tags, e.g. row_filter: region_restricted, domain: finance."})
    ]}
  ];
}

function ING_SECTIONS(){
  var srcT=function(v){return v("source_type")};
  var fmt=function(v){return v("source_config.format")};
  return flat([
    {id:"identity",title:"Flow identity",doc:"#2-ingestion-flow-schema",sub:"Identity and descriptive metadata. Everything but dataflow_id is carried to the control table without validation.",fields:[
      T("dataflow_id","dataflow_id",{req:1,ph:"df_template_ingest",i:"Unique ID for this ingestion flow. Referenced by transformation flows."}),
      T("source_system","source_system",{ph:"example_source_system",i:"Descriptive metadata — never validated."}),
      T("source_database","source_database",{ph:"example_landing_db",i:"Descriptive metadata."}),
      T("source_table_name","source_table_name",{ph:"example_raw_table",i:"Descriptive metadata."}),
      Q("source_description","source_description",{span:2,ph:"Bronze ingestion from a Volume in the {{env}} environment",i:"Becomes the target Delta table's COMMENT. Supports {{catalog}} and {{env}}."})
    ]},
    {id:"dataset",title:"Target dataset",doc:"#8-target-config-shared-by-ingestion--transformation",sub:"Where the flow lands in Unity Catalog and what kind of Lakeflow dataset is registered.",fields:[
      T("target_catalog","target_catalog",{req:1,ph:"{{catalog}}",i:"Unity Catalog catalog for the target table."}),
      T("target_schema","target_schema",{req:1,ph:"bronze_example",i:"Schema for the target table."}),
      T("target_table","target_table",{req:1,ph:"example_raw",i:"Target Delta table name."}),
      S("target_type","target_type",TARGET_TYPES,{req:1,i:"streaming_table for incremental, materialized_view for full recompute, batch_table for batch (the only type supporting iceberg), external_sink for table plus export, sink for export only."})
    ]},
    {id:"srctype",title:"Source type",doc:"#3-source-config-reference",sub:"Picking a source type decides which source_config fields apply — only those are shown below.",fields:[
      S("source_type","source_type",["","autoloader","zerobus","asn1"],{req:1,i:"autoloader reads files from a Volume, zerobus streams an existing Delta table, asn1 decodes binary CDR files."})
    ]},
    {id:"src_auto",title:"Source · autoloader",doc:"#4-source-config--autoloader-specific",w:function(v){return srcT(v)==="autoloader"},sub:"Fields specific to source_type autoloader.",fields:[
      T("source_config.path","source_config.path",{req:1,span:2,ph:"/Volumes/{{catalog}}/landing/zone/incoming/",i:"Landing directory Auto Loader monitors for new files."}),
      S("source_config.format","source_config.format",["","csv","json","parquet","avro","text","orc","binaryFile"],{req:1,i:"File format of incoming data. Maps to cloudFiles.format."}),
      T("source_config.schema_location","source_config.schema_location",{ph:"/Volumes/{{catalog}}/landing/_schemas/<table>/",i:"Where Auto Loader stores the inferred schema. If omitted a convention-based default is derived."})
    ]},
    {id:"src_zb",title:"Source · zerobus",doc:"#5-source-config--zerobus-specific",w:function(v){return srcT(v)==="zerobus"},sub:"Fields specific to source_type zerobus — a streaming read of an existing Delta table.",fields:[
      T("source_config.source_catalog","source_config.source_catalog",{req:1,ph:"example_source_catalog",i:"Catalog of the existing Delta table to stream from."}),
      T("source_config.source_schema","source_config.source_schema",{req:1,ph:"example_source_schema",i:"Schema of the source table."}),
      T("source_config.source_table","source_config.source_table",{req:1,ph:"example_source_zerobus_table",i:"Table name of the source."}),
      N("source_config.starting_version","source_config.starting_version",{ph:"0",i:"Maps to reader option startingVersion."}),
      T("source_config.max_bytes_per_trigger","source_config.max_bytes_per_trigger",{ph:"1g",i:"Throttles how much data each micro-batch reads. Maps to maxBytesPerTrigger."})
    ]},
    {id:"src_asn1",title:"Source · ASN.1",doc:"#6-source-config--asn1-specific",w:function(v){return srcT(v)==="asn1"},sub:"Fields specific to source_type asn1. The Spark schema is derived from the .asn module — no manual schema needed.",fields:[
      T("source_config.path","source_config.path",{req:1,span:2,ph:"/Volumes/{{catalog}}/landing/zone/extracted/",i:"Directory containing extracted binary CDR files."}),
      T("source_config.asn1_schema_path","source_config.asn1_schema_path",{req:1,ph:"/Volumes/{{catalog}}/landing/_asn1_schemas/cdr.asn",i:"ASN.1 module definition file. Must be a real .asn file."}),
      S("source_config.asn1_codec","source_config.asn1_codec",["","ber","der"],{req:1,i:"Which ASN.1 encoding to decode."}),
      T("source_config.asn1_pdu_name","source_config.asn1_pdu_name",{ph:"CallDetailRecord",i:"Optional. The top-level SEQUENCE/CHOICE type in the .asn file to decode each record as. Leave blank to auto-detect the root PDU; supply a name only to override detection."}),
      T("source_config.schema_location","source_config.schema_location",{ph:"auto-derived",i:"Auto Loader schema checkpoint for binary file discovery."})
    ]},
    {id:"src_common",title:"Source · reader options",doc:"#3-source-config-reference",sub:"Valid for every source type.",fields:[
      B("source_config.capture_technical_metadata","source_config.capture_technical_metadata",{i:"Adds __framework_source_file_name/_size/_modification_time/_metadata_headers and gates __framework_ingestion_timestamp_utc."}),
      S("source_config.schema_evolution_mode","source_config.schema_evolution_mode",["","addNewColumns","addNewColumnsWithTypeWidening","rescue","failOnNewColumns","none"],{i:"Maps to cloudFiles.schemaEvolutionMode. rescue sends new or mismatched columns to _rescued_data."}),
      T("source_config.file_pattern","source_config.file_pattern",{ph:"orc_*",i:"Glob or regex filtering which files are picked up. Maps to cloudFiles.fileNamePattern."}),
      B("source_config.remove_dups","source_config.remove_dups",{i:"Full-row dropDuplicates over every column except __framework_*-prefixed columns and _rescued_data / _metadata. Without a watermark this holds unbounded dedup state.",hint:"set the watermark below to bound state"}),
      T("source_config.dedup_watermark.event_time_column","dedup_watermark.event_time_column",{ind:1,w:function(v){return v("source_config.remove_dups")===true},ph:"updated_at",i:"Bounds dedup state on a streaming source. A TIMESTAMP column on the ingested DataFrame. Only meaningful with remove_dups: true."}),
      T("source_config.dedup_watermark.delay_threshold","dedup_watermark.delay_threshold",{ind:1,w:function(v){return v("source_config.remove_dups")===true},ph:"2 hours",i:"Spark interval string paired with dedup_watermark.event_time_column."}),
      T("source_config.schema_config_path","source_config.schema_config_path",{ph:"/Volumes/.../schema_config.json",i:"External JSON/YAML declaring explicit casts, nullability, UC column comments and renames. A directory resolves to its most recently modified file."}),
      KV("source_config.reader_options","source_config.reader_options",{span:2,i:"Passthrough to the Spark reader — one .option(key, value) per entry. Can override file_pattern and schema_evolution_mode if keys collide."})
    ]},
    {id:"src_norm",title:"Source · column normalization",doc:"#3-source-config-reference",sub:"Trims whitespace, replaces special characters with underscore, and applies the case below. Off unless enabled is set.",fields:[
      B("source_config.column_normalization.enabled","column_normalization.enabled",{i:"Switch for column normalization. When off, source column names are passed through unchanged."}),
      S("source_config.column_normalization.case","column_normalization.case",["","lower","preserve","upper"],{reason:"column_normalization.enabled is off",w:function(v){return v("source_config.column_normalization.enabled")===true},i:"Case applied to normalized column names. lower is the default; preserve keeps the source casing; upper uppercases."})
    ]},
    {id:"src_nested",title:"Source · nested data & standardization",doc:"#3-source-config-reference",sub:"Flattening applies to nested formats only. data_standardization_sql runs after explode_columns.",fields:[
      S("source_config.explode_mode","explode_columns mode",["","absent","empty","named"],{w:function(v){return srcT(v)==="asn1"||(srcT(v)==="autoloader"&&(fmt(v)==="json"||fmt(v)==="parquet"))},reason:"available only for json and parquet formats (and asn1 sources)",i:"How explode_columns is written to the spec. absent omits the key entirely — schema-preserving pass-through. empty writes explode_columns: [] — auto-flattens every nested struct and explodes every array in the schema. named writes the columns you list below.",hint:"the absent / explicitly-empty distinction is load-bearing"}),
      L("source_config.explode_columns","source_config.explode_columns",{w:function(v){return v("source_config.explode_mode")==="named"&&(srcT(v)==="asn1"||(srcT(v)==="autoloader"&&(fmt(v)==="json"||fmt(v)==="parquet")))},i:"Only the named top-level columns are processed. A column that is neither struct nor array at runtime is an error."}),
      B("source_config.auto_flatten_all","source_config.auto_flatten_all",{w:function(v){return (srcT(v)==="asn1"||(srcT(v)==="autoloader"&&(fmt(v)==="json"||fmt(v)==="parquet")))&&v("source_config.explode_mode")!=="named"},reason:"overridden while explode_columns names specific columns",i:"Recursively flattens all nested structs and explodes all arrays regardless of explode_columns — the same effect as an explicitly-empty explode_columns. Only takes effect while explode_columns is absent or empty.",hint:"same effect as the empty explode_columns mode"}),
      L("source_config.json_string_columns","source_config.json_string_columns",{span:2,w:function(v){return srcT(v)==="autoloader"&&(fmt(v)==="json"||fmt(v)==="parquet")},reason:"available only for json and parquet formats",i:"STRING columns holding a JSON document, parsed with from_json / schema_of_json before flattening — giving Parquet sources parity with JSON. Each entry is either a column name or an object of the form {\"column\": \"col\", \"schema_ddl\": \"struct<...>\"}.",hint:"name, or {column, schema_ddl} for an explicit schema"}),
      L("source_config.data_standardization_sql","source_config.data_standardization_sql",{span:2,ph:"trim(region) AS region, upper(country_code) AS country_code",i:"Per-column expressions, each ending AS <column>. Restricted grammar: no SELECT/FROM/JOIN/UNION/WHERE/DML/DDL and no semicolons."})
    ]},
    {id:"retention",title:"Source · landing retention",doc:"#3-source-config-reference",sub:"What happens to landing-zone files after ingestion. Maps to cloudFiles.cleanSource.",fields:[
      S("source_config.landing_retention_policy.clean_source","landing_retention_policy.clean_source",["","off","archive","delete"],{i:"off leaves files in place, archive moves them, delete removes them."}),
      T("source_config.landing_retention_policy.archive_path","landing_retention_policy.archive_path",{w:function(v){return v("source_config.landing_retention_policy.clean_source")==="archive"},ph:"/Volumes/{{catalog}}/landing/_archive/zone/",i:"Where archived files are moved. Maps to cloudFiles.cleanSource.moveDestination. No longer a hard requirement: archive with a missing or empty archive_path degrades to off with a runtime warning. delete never needs it.",hint:"missing path degrades archive to off — not a validation error"}),
      N("source_config.landing_retention_policy.retention_days","landing_retention_policy.retention_days",{w:function(v){return v("source_config.landing_retention_policy.clean_source")!=="off"},ph:"7",i:"Maps to cloudFiles.cleanSource.retentionDuration as N days. Minimum 0; defaults to 7 when omitted."})
    ]},
    {id:"zip",title:"Source · ZIP handling",doc:"#7-source-zip-handling",w:function(v){return srcT(v)==="autoloader"||srcT(v)==="asn1"},sub:"Extracts, and optionally PGP-decrypts, a ZIP into source_config.path before the reader runs. Enable it first, then configure.",fields:flat([
      B("source_config.source_zip_handling.enabled","source_zip_handling.enabled",{req:1,span:2,i:"Master switch for ZIP extraction."}),
      T("source_config.source_zip_handling.source_zip_path","source_zip_handling.source_zip_path",{req:1,ind:1,w:function(v){return v("source_config.source_zip_handling.enabled")===true},ph:"/Volumes/{{catalog}}/landing/zone/incoming/",i:"Directory where ZIP files are found — never a single file."}),
      T("source_config.source_zip_handling.zip_file_pattern","source_zip_handling.zip_file_pattern",{req:1,ind:1,w:function(v){return v("source_config.source_zip_handling.enabled")===true},ph:"*.zip",i:"Which ZIP files to pick up."}),
      S("source_config.source_zip_handling.member_format","source_zip_handling.member_format",["","zip","gzip"],{ind:1,w:function(v){return v("source_config.source_zip_handling.enabled")===true},hint:"unset = zip",i:"The landing archive's CONTAINER, orthogonal to any decryption layer. \u0027zip\u0027 (the default when absent) is a real archive with N named members, opened by pyzipper. \u0027gzip\u0027 is a single compressed stream with no member table, which pyzipper cannot open at all. An UNENCRYPTED .gz needs no ZIP handling whatsoever \u2014 Spark decompresses it on read. Choose gzip only for an ENCRYPTED .gz, e.g. .csv.gz.gpg."}),
      T("source_config.source_zip_handling.target_volume_path","source_zip_handling.target_volume_path",{req:1,ind:1,w:function(v){return v("source_config.source_zip_handling.enabled")===true},ph:"/Volumes/{{catalog}}/landing/zone/extracted/",i:"Where extracted files are written. Usually matches source_config.path."}),
      S("source_config.source_zip_handling.delete_source_after_extract.action","delete_source_after_extract.action",["","delete_now","delete_after_x_days"],{ind:1,w:function(v){return v("source_config.source_zip_handling.enabled")===true},i:"What happens to the ZIP after a successful extraction. delete_now removes the archive as soon as its members are extracted; delete_after_x_days runs an age-based sweep of the landing directory, skipping only archives whose extraction failed in the same run. Leaving this unset keeps the archive.",hint:"unset keeps the archive"}),
      N("source_config.source_zip_handling.delete_source_after_extract.days","delete_source_after_extract.days",{req:1,ind:2,w:function(v){return v("source_config.source_zip_handling.delete_source_after_extract.action")==="delete_after_x_days"},ph:"7",i:"Age in days after which a successfully-extracted archive is swept."}),
      S("source_config.source_zip_handling.pre_extraction_decryption.type","pre_extraction_decryption.type",["","pgp","pgp_symmetric"],{ind:1,w:function(v){return v("source_config.source_zip_handling.enabled")===true},i:"Outer decryption layer applied before extraction. \u0027pgp\u0027 decrypts a message encrypted to a RECIPIENT KEYPAIR (private_key_secret required). \u0027pgp_symmetric\u0027 decrypts a PASSPHRASE-encrypted message \u2014 what `gpg --symmetric --cipher-algo AES256` produces \u2014 and requires passphrase_secret instead. The two are mutually exclusive at the message level: a key-encrypted message is not passphrase-decryptable, and vice versa."}),
      SK("source_config.source_zip_handling.pre_extraction_decryption.private_key_secret","pre_extraction_decryption.private_key_secret",1).map(function(f){return Object.assign({},f,{w:function(v){return v("source_config.source_zip_handling.pre_extraction_decryption.type")==="pgp"}})}),
      SK("source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret","pre_extraction_decryption.passphrase_secret",0).map(function(f){return Object.assign({},f,{reason:"pre_extraction_decryption.type is not pgp or pgp_symmetric",i:"Depends on type. Under \u0027pgp_symmetric\u0027 this is REQUIRED and is the passphrase the OpenPGP message itself was encrypted with (`gpg --symmetric`). Under \u0027pgp\u0027 it is OPTIONAL and protects the PRIVATE KEY above. Neither is the ZIP\u0027s AES password: that is secret_passphrase, below.",w:function(v){var t=v("source_config.source_zip_handling.pre_extraction_decryption.type");return t==="pgp"||t==="pgp_symmetric"}})}),
      SK("source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase","pre_extraction_decryption.secret_passphrase",0).map(function(f){return Object.assign({},f,{reason:"pre_extraction_decryption.type is not set",i:"AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time. Independent of, and combinable with, the PGP layer above. Not the PGP key's passphrase: that is passphrase_secret.",w:function(v){return !!v("source_config.source_zip_handling.pre_extraction_decryption.type")&&v("source_config.source_zip_handling.member_format")!=="gzip"}})})
    ])},
    TARGET_SECTIONS("ingestion")
  ]);
}

function TRN_SECTIONS(){return flat([
  {id:"identity",title:"Flow identity",doc:"#13-transformation-flow-fields",sub:"Silver and Gold transformation step. dataflow_id is a dependency label only.",fields:[
    T("flow_step_id","flow_step_id",{req:1,ph:"ts_template_scd1_example",i:"Unique ID for this transformation step."}),
    T("dataflow_id","dataflow_id",{req:1,ph:"df_template_ingest",i:"Which ingestion flow this logically follows."})
  ]},
  {id:"dataset",title:"Target dataset",doc:"#8-target-config-shared-by-ingestion--transformation",sub:"Where this step lands.",fields:[
    T("target_catalog","target_catalog",{req:1,ph:"{{catalog}}",i:"Unity Catalog catalog for the target table."}),
    T("target_schema","target_schema",{req:1,ph:"silver_example",i:"Schema for the target table."}),
    T("target_table","target_table",{req:1,ph:"example_dim_scd1",i:"Target table name."}),
    S("target_type","target_type",TARGET_TYPES,{req:1,i:"Same five values as ingestion."})
  ]},
  {id:"inputs",title:"Source inputs",doc:"#14-source-inputs-transformation_flowssource_inputs",sub:"Upstream tables. input_name must be unique across the entire spec and is what transformation_sql joins on. Watermarks are required when a stream is joined to another stream.",fields:[
    REP("source_inputs","source_inputs[]",[
      T("input_name","input_name",{req:1,ph:"example_raw_input",i:"Must be unique across the entire spec, not just this flow. Referenced in transformation_sql FROM/JOIN."}),
      T("table","table",{req:1,ph:"{{catalog}}.bronze_example.example_raw",i:"Fully-qualified upstream table. Can be any table, not only ones produced by this spec."}),
      B("is_streaming","is_streaming",{i:"true reads with spark.readStream.table, false with spark.read.table."}),
      T("watermark.event_time_column","watermark.event_time_column",{w:function(v){return v("is_streaming")===true},ph:"updated_at",i:"Event-time column for watermarking. Auto-cast to timestamp. Required when streaming and joined with another stream."}),
      T("watermark.delay_threshold","watermark.delay_threshold",{w:function(v){return v("is_streaming")===true},ph:"10 minutes",i:"Maximum allowed event lateness. Required when streaming and joined with another stream."})
    ],{span:2,addLabel:"+ add input"})
  ]},
  {id:"decrypt",title:"Decrypted columns",doc:"#15-decrypted-columns-source_inputsdecrypted_columns",sub:"The only valid place to decrypt — never in target_config. Runs before transformation_sql.",fields:[
    REP("decrypted_columns","source_inputs[].decrypted_columns[]",DEC_FIELDS,{span:2,addLabel:"+ decrypt a column"})
  ]},
  {id:"sql",title:"Transformation SQL",doc:"#13-transformation-flow-fields",sub:"Native Spark SQL. Supports ${param} substitution, UNION/UNION ALL, and automatic STREAM injection for streaming inputs. Do not wrap ${param} in quotes.",fields:[
    Q("transformation_sql","transformation_sql",{req:1,span:2,ph:"SELECT example_id, region, amount FROM example_raw_input WHERE amount > ${min_amount}",i:"The SQL transform. Reads from input_name values declared in source_inputs."})
  ]},
  TARGET_SECTIONS("transformation")
])}

// Reconciliation execution modes (v1.5.0). "job" is and stays the default: existing job
// resources already run reconciliation tasks against onboarded rows, so a pipeline default
// would run those flows twice per cycle. The two pipeline modes register the flow inside its
// dataflow group's Lakeflow pipeline update instead.
var EXEC_MODES=["","job","pipeline","pipeline_audit_only"];
var REC_PIPELINE_MODES=["pipeline","pipeline_audit_only"];
// Mirrors the JSON registry's {"in": ["execution_mode", [...]]} visible_when. Deliberately an
// `in` test, not `execution_mode !== "job"`: an unset value must read as job mode, and a !==
// test would show every pipeline-only field on a blank flow.
function isRecPipelineMode(v){return REC_PIPELINE_MODES.indexOf(v("execution_mode"))>-1}

function RECON_DATASET(prefix,label){return [
  S(prefix+".type",label+".type",["","table"],{i:"Reconciliation is scoped to Delta tables only. Any other value is rejected outright — path-based file and sink sources are no longer valid here."}),
  T(prefix+".table",label+".table",{req:1,ph:"{{catalog}}.bronze.table",i:"Three-part fully-qualified table name."}),
  S(prefix+".read_mode",label+".read_mode",["","batch","streaming"],{i:"At most one side of a given target's comparison may be streaming. Rejected on presence with value 'streaming' when execution_mode is 'pipeline' or 'pipeline_audit_only': the in-pipeline comparison is a whole-snapshot batch classification, and a stream-static join supports only inner and left_outer, which cannot express MISSING_IN_SOURCE. Use read_mode 'batch' (the default), or set execution_mode to 'job' to keep the standalone streaming engine."}),
  T(prefix+".task_run_id_column",label+".task_run_id_column",{ph:"__framework_pipeline_run_id",i:"When the task_run_id job parameter is set, narrows this side's read to that run's rows. Reconciliation is triggered-only as of v1.4.0. Rejected on presence when execution_mode is 'pipeline' or 'pipeline_audit_only': no stable per-update key exists inside a Lakeflow update, so the narrowing would match every row that pipeline ever wrote -- a silent no-op. Use filter_condition, or set execution_mode to 'job'."}),
  Q(prefix+".filter_condition",label+".filter_condition",{span:2,ph:"load_date = '${run_date}'",i:"Boolean SQL applied after read. Supports ${param} substitution."}),
  L(prefix+".data_standardization_sql",label+".data_standardization_sql",{span:2,ph:"trim(status) AS status",i:"Column-level cleanup before matching. Same restricted grammar as ingestion."}),
  B(prefix+".hash_precomputed",label+".hash_precomputed",{i:"Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing. Only valid for type table."})
]}

function REC_SECTIONS(){return [
  {id:"identity",title:"Reconciliation identity",doc:"#16-reconciliation-flow-fields",sub:"Cross-dataset comparison and self-healing.",fields:[
    T("reconciliation_id","reconciliation_id",{req:1,ph:"recon_template_example",i:"Unique ID for this reconciliation flow."}),
    S("execution_mode","execution_mode",EXEC_MODES,{i:"Where this reconciliation flow runs, and the field that gates which of the fields below are legal. job runs it as a 05_reconciliation_engine.py job task, exactly as today, and is the default because existing job resources already run reconciliation tasks against onboarded rows. pipeline registers it inside its dataflow group's Lakeflow pipeline update as a third flow type -- the published classified/metrics/mismatch datasets and the heal (append-back) lane. pipeline_audit_only registers the comparison, metrics and dq_config expectations in-pipeline but leaves healing in job mode. Both pipeline modes require dataflow_group_id and read_mode batch on every side. Only pipeline additionally requires an append-only source producer, because it streams the source to drive the heal lane; pipeline_audit_only reads the source as a batch and is the correct mode when the source is written by SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC or is a fully refreshed materialized view."}),
    T("dataflow_group_id","dataflow_group_id",{req:1,w:isRecPipelineMode,ph:"dfg_example_group",i:"The dataflow group whose Lakeflow pipeline this flow is registered into. Required when execution_mode is pipeline or pipeline_audit_only -- a group-less reconciliation flow has no pipeline update to live in. Usually this spec's own dataflow_group_id; naming another group registers the flow inside that group's pipeline instead. Stays optional in job mode, where the standalone engine handles the group-less case."}),
    T("publish_schema","publish_schema",{w:isRecPipelineMode,ph:"recon_example",i:"Schema, inside the hosting pipeline's own catalog, where this flow's recon__<reconciliation_id>__<target_id>__metrics and __mismatch datasets are published. Since v1.7.07 this is the only thing that publishes: leave it empty and the audit datasets stay pipeline-scoped (a dq_config gate still works, nothing is exported). Required when run_log_capture or mismatch_log_capture is true, and for a healing pipeline-mode flow. Rejected on presence when execution_mode is job -- a job-mode flow publishes none of those datasets."}),
    B("two_tier_verification","two_tier_verification",{i:"Runs a cheap Phase 1 per-side fingerprint (row_count plus an XOR-fold of the framework hash columns) (bit_xor(hash) plus a per-side count) first, and only falls through to the full matcher join when that phase disagrees.",hint:"phase 1 fingerprint, phase 2 full join"}),
    B("logging_config.run_log_capture","logging_config.run_log_capture",{i:"Per-flow gate on run-log writes. DEFAULTS TO FALSE since v1.7.3 (it defaulted to true through v1.7.2): reconciliation is silent by default, so leaving this unset means no reconciliation_run_log or reconciliation_result rows and, in pipeline mode, no recon__*__metrics dataset at all. Set it true to opt in -- and you MUST set it true when the flow declares dq_config.rules, whose expectations attach to that dataset. Overridable at runtime by the recon_run_log_capture job parameter.",hint:"v1.7.3: defaults false -- auditing is opt-in"}),
    B("logging_config.mismatch_log_capture","logging_config.mismatch_log_capture",{i:"Per-flow gate on mismatch-log writes. DEFAULTS TO FALSE since v1.7.3 (it defaulted to true through v1.7.2): leaving this unset means no reconciliation_mismatch_log rows and, in pipeline mode, no recon__*__mismatch dataset. Set it true to opt in. Overridable at runtime by the recon_mismatch_log job parameter.",hint:"v1.7.3: defaults false -- auditing is opt-in"}),
    S("error_handling.on_failure","error_handling.on_failure",["","fail","warn"],{i:"fail raises an error, warn logs and continues."})
  ]},
  {id:"rsource",title:"Source dataset",doc:"#17-reconciliation-dataset-config-shared-source--target",half:1,cols:1,sub:"source_config — the baseline to compare from.",fields:flat(RECON_DATASET("source_config","source_config"))},
  {id:"rtargets",title:"Target dataset",doc:"#18-reconciliation-target-only-fields",half:1,cols:1,sub:"One reconciliation flow compares one source against one target table, resolved from catalog, schema and table.",fields:[
    REP("target_configs","target_configs[]",[
      T("target_id","target_id",{req:1,span:2,ph:"primary_product_table",i:"Identifies this target in run and mismatch logs."}),
      T("target_catalog","catalog",{req:1,ph:"{{catalog}}",i:"Unity Catalog catalog of the target table. Composed into the three-part table name on save."}),
      T("target_schema","schema",{req:1,ph:"bronze_example",i:"Schema of the target table. Composed into the three-part table name on save."}),
      T("target_table","table",{req:1,ph:"example_raw_final",i:"Target table name. Composed into the three-part table name on save."}),
      S("read_mode","read_mode",["","batch","streaming"],{i:"At most one side of a given target's comparison may be streaming. Rejected on presence with value 'streaming' when execution_mode is 'pipeline' or 'pipeline_audit_only': the in-pipeline comparison is a whole-snapshot batch classification, and a stream-static join supports only inner and left_outer, which cannot express MISSING_IN_SOURCE."}),
      T("task_run_id_column","task_run_id_column",{ph:"__framework_pipeline_run_id",i:"When the task_run_id job parameter is set, narrows this side's read to that run's rows. Reconciliation is triggered-only as of v1.4.0. Rejected on presence when execution_mode is 'pipeline' or 'pipeline_audit_only': no stable per-update key exists inside a Lakeflow update, so the narrowing would be a silent no-op. Use filter_condition instead."}),
      Q("filter_condition","filter_condition",{span:2,ph:"load_date = '${run_date}'",i:"Boolean SQL applied after read. Supports ${param} substitution."}),
      L("data_standardization_sql","data_standardization_sql",{span:2,ph:"trim(status) AS status",i:"Column-level cleanup before matching. Same restricted grammar as ingestion."}),
      B("hash_precomputed","hash_precomputed",{i:"Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing. Valid because reconciliation targets are always tables."}),
      S("comparison_direction","comparison_direction",["","both","source_to_target","target_to_source"],{i:"source_to_target self-heals missing records, target_to_source audits orphaned records, both does each."}),
      T("append_target_table","append_target_table",{req:1,span:2,w:function(v){return v("comparison_direction")!=="target_to_source"},ph:"{{catalog}}.bronze_example.example_raw_cdc",i:"Where self-healed records are appended. Required when the direction includes source-to-target."})
    ],{span:2,single:1})
  ]},
  {id:"matching",title:"Matching & healing",doc:"#16-reconciliation-flow-fields",sub:"How records are paired, which columns are compared, and how missing rows are reshaped before append.",fields:[
    L("match_keys","match_keys",{req:1,ph:"example_id",i:"Columns identifying the same logical record across datasets."}),
    L("compare_columns","compare_columns",{ph:"amount, status",i:"Columns compared for drift after key matching. Defaults to all columns."}),
    Q("transform_sql","transform_sql",{span:2,ph:"SELECT example_id, amount, status FROM _reconciliation_unmatched_records",i:"Reshapes missing records before append when source and target schemas differ. Must read FROM _reconciliation_unmatched_records. Full Spark SQL, not the restricted grammar."})
  ]},
  {id:"rdq",title:"Reconciliation data quality",doc:"#16-reconciliation-flow-fields",sub:"dq_config — expectations attached to this flow's one-row __metrics dataset. The first declarative way a reconciliation threshold can fail a pipeline update. In-pipeline execution modes only.",fields:[
    REP("dq_config.rules","dq_config.rules[]",[
      T("rule_id","rule_id",{req:1,ph:"no_value_drift",i:"Unique expectation identifier."}),
      S("action","action",["","warn","drop","fail"],{req:1,i:"warn logs and keeps the metrics row, drop removes it, fail aborts the pipeline update. quarantine is deliberately not offered: it is rejected for a reconciliation flow because a one-row metrics dataset has nothing to quarantine."}),
      Q("expression","expression",{req:1,span:2,ph:"value_drift_count = 0",i:"Boolean Spark SQL expression over the metrics columns (source_row_count, target_row_count, missing_in_target_count, missing_in_source_count, value_drift_count)."})
    ],{span:2,w:isRecPipelineMode,i:"Expectations evaluated against the one-row metrics dataset this flow publishes, e.g. value_drift_count = 0. Additive: it does not repurpose error_handling.on_failure, which keeps its exception-level try/except meaning. Rejected on presence when execution_mode is job -- a job task has no dataset to attach expectations to."})
  ]}
]}

var PRESETS = {
  ing: [
    ["blank","Blank ingestion flow","Nothing filled in — cascade from source_type.",{v:{}}],
    ["autoloader_csv","autoloader · csv · APPEND","Volume ingestion with regex file selection, rescue schema evolution, archive retention, quarantine, encryption and standardization.",{
      v:{dataflow_id:"df_template_ingest",source_system:"example_source_system",source_database:"example_landing_db",source_table_name:"example_raw_table",source_description:"Template: Bronze ingestion from a Volume (Auto Loader) with regex file selection, quarantine, encryption, and data standardization, in the {{env}} environment",source_type:"autoloader",target_catalog:"{{catalog}}",target_schema:"bronze_example",target_table:"example_raw",target_type:"streaming_table",
        "source_config.path":"/Volumes/{{catalog}}/landing/example_raw_zone/incoming/","source_config.format":"csv","source_config.file_pattern":"orc_*","source_config.schema_location":"/Volumes/{{catalog}}/landing/_schemas/example_raw/","source_config.schema_evolution_mode":"rescue","source_config.capture_technical_metadata":true,"source_config.data_standardization_sql":"trim(region) AS region, upper(country_code) AS country_code",
        "source_config.landing_retention_policy.clean_source":"archive","source_config.landing_retention_policy.archive_path":"/Volumes/{{catalog}}/landing/_archive/example_raw_zone/","source_config.landing_retention_policy.retention_days":"7",
        "target_config.cdc_load_strategy":"APPEND","target_config.storage_format":"delta","target_config.partition_mode":"named","target_config.partition_columns":"region","target_config.liquid_clustering_columns":"example_id","target_config.auto_ttl.timestamp_column":"updated_at","target_config.auto_ttl.expire_in_days":"90",
        "dq_config.quarantine_table":"example_raw_quarantine","dq_config.record_id_column":"example_id"},
      kvs:{"source_config.reader_options":[["header","true"],["cloudFiles.inferColumnTypes","true"]],"target_config.table_properties":[["log_retention_duration","interval 30 days"],["deleted_file_retention_duration","interval 30 days"]],"governance_tags.table_tags":[["row_filter","region_restricted"],["domain","example"]]},
      reps:{"target_config.encrypted_columns":[{column_name:"pii_column",output_column:"pii_column",mode:"GCM",source_data_type:"string","secret.secret_catalog":"{{catalog}}","secret.secret_schema":"security","secret.secret_key":"pii_encryption_key"}],
        "dq_config.rules":[{rule_id:"dq_example_id_not_null",expression:"example_id IS NOT NULL",action:"drop"},{rule_id:"dq_amount_non_negative",expression:"amount >= 0",action:"quarantine"}],
        "governance_tags.column_tags":[{column:"pii_column",__kv:{tags:[["mask","PII"],["classification","restricted"]]}}]}
    }],
    ["zerobus_scd1","zerobus · SCD1","Streaming read of a Delta table landed by Zerobus direct-write, with delete-marking, hash columns and wide-source exclusions.",{
      v:{dataflow_id:"df_template_zerobus_ingest",source_system:"example_zerobus_direct_write",source_database:"example_events_db",source_table_name:"example_zerobus_events",source_description:"Template: streaming read of an existing Delta table landed by Zerobus direct-write, SCD1 with an optional sequence column, delete-marking, hash columns, and a wide-source comparison exclusion",source_type:"zerobus",target_catalog:"{{catalog}}",target_schema:"bronze_example",target_table:"example_zerobus_events",target_type:"streaming_table",
        "source_config.source_catalog":"example_source_catalog","source_config.source_schema":"example_source_schema","source_config.source_table":"example_source_zerobus_table","source_config.starting_version":"0","source_config.max_bytes_per_trigger":"1g","source_config.capture_technical_metadata":true,
        "target_config.cdc_load_strategy":"SCD1","target_config.primary_keys":"event_id","target_config.cdc_operation_column":"op","target_config.cdc_operation_mapping.delete_values":"D","target_config.columns_to_exclude":"batch_load_ts, source_extract_filename","target_config.generate_hash_columns":true},
      reps:{"dq_config.rules":[{rule_id:"dq_example_event_status_known_value",expression:"event_status IN ('OPEN', 'CLOSED')",action:"fail"}]}
    }],
    ["asn1_snapshot","asn1 · PGP ZIP · FULL_SNAPSHOT_CDC","Binary BER/DER CDR ingestion, PGP-decrypted then unzipped, snapshot diff on the CDR's own callReferenceId.",{
      v:{dataflow_id:"df_template_asn1_ingest",source_system:"example_telecom_switch",source_database:"example_cdr_landing",source_table_name:"example_asn1_cdr",source_description:"Template: binary ASN.1 (BER/DER) CDR ingestion, PGP-decrypted then unzipped at rest in the source, full-snapshot CDC diffed on the CDR's own callReferenceId",source_type:"asn1",target_catalog:"{{catalog}}",target_schema:"bronze_example",target_table:"example_asn1_cdr",target_type:"batch_table",
        "source_config.path":"/Volumes/{{catalog}}/landing/example_asn1_zone/extracted/","source_config.schema_location":"/Volumes/{{catalog}}/landing/_schemas/example_asn1_cdr/","source_config.asn1_schema_path":"/Volumes/{{catalog}}/landing/_asn1_schemas/example_cdr.asn","source_config.asn1_codec":"ber","source_config.asn1_pdu_name":"ExampleCallDetailRecord","source_config.capture_technical_metadata":true,
        "source_config.source_zip_handling.enabled":true,"source_config.source_zip_handling.source_zip_path":"/Volumes/{{catalog}}/landing/example_asn1_zone/{{env}}/incoming/","source_config.source_zip_handling.zip_file_pattern":"example_cdr_batch.zip","source_config.source_zip_handling.target_volume_path":"/Volumes/{{catalog}}/landing/example_asn1_zone/extracted/","source_config.source_zip_handling.delete_source_after_extract":true,
        "source_config.source_zip_handling.pre_extraction_decryption.type":"pgp","source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_catalog":"{{catalog}}","source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_schema":"security","source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_key":"cdr_pgp_private_key","source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_catalog":"{{catalog}}","source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_schema":"security","source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_key":"cdr_zip_passphrase",
        "target_config.cdc_load_strategy":"FULL_SNAPSHOT_CDC","target_config.primary_keys":"callReferenceId","target_config.storage_format":"delta","target_config.cdc_operation_column":"recordStatus","target_config.cdc_operation_mapping.delete_values":"DELETED"},
      reps:{"dq_config.rules":[{rule_id:"dq_example_call_duration_non_negative",expression:"callDurationSeconds >= 0",action:"warn"}]}
    }],
    ["json_explode","autoloader · json · explode_columns","JSON ingestion that flattens only the named nested column and leaves everything else as-is.",{
      v:{dataflow_id:"df_template_json_ingest",source_system:"example_events_api",source_table_name:"example_events_json",source_description:"Template: JSON ingestion demonstrating explode_columns -- flatten only the named nested columns, leave everything else as-is",source_type:"autoloader",target_catalog:"{{catalog}}",target_schema:"bronze_example",target_table:"example_events_json",target_type:"streaming_table",
        "source_config.path":"/Volumes/{{catalog}}/landing/example_json_zone/incoming/","source_config.format":"json","source_config.schema_location":"/Volumes/{{catalog}}/landing/_schemas/example_events_json/","source_config.explode_mode":"named","source_config.explode_columns":"event_payload","source_config.capture_technical_metadata":true,"target_config.cdc_load_strategy":"APPEND"}
    }]
  ],
  trn: [
    ["blank","Blank transformation flow","Nothing filled in — cascade from target_type and load strategy.",{v:{}}],
    ["append","APPEND · filtered select","Streaming input, parameterised WHERE, partitioned APPEND target.",{
      v:{flow_step_id:"ts_template_append_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_events_append",target_type:"streaming_table",transformation_sql:"SELECT example_id, region, amount, updated_at FROM example_events_append_src WHERE amount > ${min_amount}","target_config.cdc_load_strategy":"APPEND","target_config.storage_format":"delta","target_config.partition_mode":"named","target_config.partition_columns":"region"},
      reps:{source_inputs:[{input_name:"example_events_append_src",table:"{{catalog}}.bronze_example.example_raw",is_streaming:true}]}
    }],
    ["mv_truncate","TRUNCATE_AND_LOAD · materialized_view","Full recompute aggregate over a batch read.",{
      v:{flow_step_id:"ts_template_truncate_and_load_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_dim_truncate_reload",target_type:"materialized_view",transformation_sql:"SELECT region, COUNT(*) AS example_count FROM example_dim_truncate_reload_src GROUP BY region","target_config.cdc_load_strategy":"TRUNCATE_AND_LOAD"},
      reps:{source_inputs:[{input_name:"example_dim_truncate_reload_src",table:"{{catalog}}.bronze_example.example_raw",is_streaming:false}]}
    }],
    ["scd1_wide","SCD1 · wide source","Delete-marking, hash columns, and five audit columns excluded from schema and comparison.",{
      v:{flow_step_id:"ts_template_scd1_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_dim_scd1_wide",target_type:"streaming_table",transformation_sql:"SELECT * FROM example_dim_scd1_wide_src","target_config.cdc_load_strategy":"SCD1","target_config.primary_keys":"example_id","target_config.sequence_by_column":"updated_at","target_config.cdc_operation_column":"op","target_config.cdc_operation_mapping.delete_values":"D","target_config.columns_to_exclude":"batch_load_ts, source_extract_filename, etl_run_id, checksum_hash, ingestion_notes","target_config.generate_hash_columns":true},
      reps:{source_inputs:[{input_name:"example_dim_scd1_wide_src",table:"{{catalog}}.bronze_example.example_raw",is_streaming:true}]}
    }],
    ["scd2_crypto","SCD2 · decrypt then re-encrypt","History on one compared column, PII decrypted on read and re-encrypted on write.",{
      v:{flow_step_id:"ts_template_scd2_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_dim_scd2",target_type:"streaming_table",transformation_sql:"SELECT example_id, region, amount, updated_at, pii_column_plain FROM example_raw_input WHERE region = ${filter_country}","target_config.cdc_load_strategy":"SCD2","target_config.primary_keys":"example_id","target_config.sequence_by_column":"updated_at","target_config.columns_to_check":"amount","target_config.cdc_operation_column":"op","target_config.cdc_operation_mapping.delete_values":"D","target_config.generate_hash_columns":true},
      reps:{source_inputs:[{input_name:"example_raw_input",table:"{{catalog}}.bronze_example.example_raw",is_streaming:true}],
        decrypted_columns:[{input_name:"example_raw_input",column_name:"pii_column",output_column:"pii_column_plain",mode:"GCM",cast_to_type:"string","secret.secret_catalog":"{{catalog}}","secret.secret_schema":"security","secret.secret_key":"pii_encryption_key"}],
        "target_config.encrypted_columns":[{column_name:"pii_column_plain",output_column:"pii_column",mode:"GCM",source_data_type:"string","secret.secret_catalog":"{{catalog}}","secret.secret_schema":"security","secret.secret_key":"pii_encryption_key"}]}
    }],
    ["scd3","SCD3 · current + previous","Transformation-only strategy pivoting one status column.",{
      v:{flow_step_id:"ts_template_scd3_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_dim_scd3",target_type:"streaming_table",transformation_sql:"SELECT example_id, status_code, updated_at FROM example_dim_scd3_src","target_config.cdc_load_strategy":"SCD3","target_config.primary_keys":"example_id","target_config.sequence_by_column":"updated_at","target_config.columns_to_check":"status_code"},
      reps:{source_inputs:[{input_name:"example_dim_scd3_src",table:"{{catalog}}.bronze_example.example_raw",is_streaming:true}]}
    }],
    ["snapshot_keyed","FULL_SNAPSHOT_CDC · keyed snapshot diff","Snapshot diff over a declared primary key -- the Databricks-native apply_changes_from_snapshot pattern.",{
      v:{flow_step_id:"ts_template_full_snapshot_cdc_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_dim_full_snapshot",target_type:"streaming_table",transformation_sql:"SELECT region, amount FROM example_dim_full_snapshot_src","target_config.cdc_load_strategy":"FULL_SNAPSHOT_CDC","target_config.primary_keys":"region"},
      reps:{source_inputs:[{input_name:"example_dim_full_snapshot_src",table:"{{catalog}}.bronze_example.example_raw",is_streaming:false}]}
    }],
    ["external_sink","external_sink · pgp_zip export","Governed table plus a signed, password-protected PGP ZIP export.",{
      v:{flow_step_id:"ts_template_external_sink_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_export_staged",target_type:"external_sink",transformation_sql:"SELECT example_id, region, amount, updated_at FROM example_export_staged_src","target_config.cdc_load_strategy":"APPEND","target_config.sink_config.format":"pgp_zip","target_config.sink_config.path":"/Volumes/{{catalog}}/egress/example_export/{{env}}/_staging/","target_config.sink_config.post_export_archive.enabled":true,"target_config.sink_config.post_export_archive.output_zip_path":"/Volumes/{{catalog}}/egress/zips/example_export/{{env}}/","target_config.sink_config.post_export_archive.secret.secret_catalog":"{{catalog}}","target_config.sink_config.post_export_archive.secret.secret_schema":"security","target_config.sink_config.post_export_archive.secret.secret_key":"egress_zip_password","target_config.sink_config.post_export_archive.pgp_encryption.enabled":true,"target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_catalog":"{{catalog}}","target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_schema":"security","target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_key":"egress_recipient_public_key","target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_catalog":"{{catalog}}","target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_schema":"security","target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_key":"egress_sender_private_key"},
      reps:{source_inputs:[{input_name:"example_export_staged_src",table:"{{catalog}}.bronze_example.example_raw",is_streaming:true}]}
    }],
    ["pure_sink","sink · delta export","Pure export, no intermediate table.",{
      v:{flow_step_id:"ts_template_pure_sink_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_direct_sink_output",target_type:"sink",transformation_sql:"SELECT example_id, region, amount, updated_at FROM example_direct_sink_src","target_config.cdc_load_strategy":"APPEND","target_config.sink_config.format":"delta","target_config.sink_config.path":"/Volumes/{{catalog}}/egress/example_direct_sink/{{env}}/","target_config.sink_config.write_mode":"append"},
      reps:{source_inputs:[{input_name:"example_direct_sink_src",table:"{{catalog}}.bronze_example.example_raw",is_streaming:true}]}
    }],
    ["stream_join","APPEND · stream-stream join","Two watermarked streams joined on an interval predicate.",{
      v:{flow_step_id:"ts_template_stream_join_watermark_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_dim_stream_join",target_type:"streaming_table",transformation_sql:"SELECT l.example_id, r.event_id, l.amount, r.event_ts FROM example_raw_join_left l JOIN example_zerobus_join_right r ON l.example_id = r.example_id AND r.event_ts BETWEEN l.updated_at - INTERVAL 10 MINUTES AND l.updated_at + INTERVAL 10 MINUTES","target_config.cdc_load_strategy":"APPEND"},
      reps:{source_inputs:[{input_name:"example_raw_join_left",table:"{{catalog}}.bronze_example.example_raw",is_streaming:true,"watermark.event_time_column":"updated_at","watermark.delay_threshold":"10 minutes"},{input_name:"example_zerobus_join_right",table:"{{catalog}}.bronze_example.example_zerobus_events",is_streaming:true,"watermark.event_time_column":"event_ts","watermark.delay_threshold":"10 minutes"}]}
    }],
    ["union_all","APPEND · UNION ALL","Two streaming inputs combined with UNION ALL.",{
      v:{flow_step_id:"ts_template_union_all_example",dataflow_id:"df_template_ingest",target_catalog:"{{catalog}}",target_schema:"silver_example",target_table:"example_union_all_combined",target_type:"streaming_table",transformation_sql:"SELECT example_id, region, amount FROM example_union_left UNION ALL SELECT event_id AS example_id, NULL AS region, CAST(NULL AS DOUBLE) AS amount FROM example_union_right","target_config.cdc_load_strategy":"APPEND"},
      reps:{source_inputs:[{input_name:"example_union_left",table:"{{catalog}}.bronze_example.example_raw",is_streaming:true},{input_name:"example_union_right",table:"{{catalog}}.bronze_example.example_zerobus_events",is_streaming:true}]}
    }]
  ],
  rec: [
    ["blank","Blank reconciliation flow","Nothing filled in.",{v:{}}],
    ["full","Table vs table · self-healing","Filtered baseline compared both ways, unmatched rows reshaped and appended, failure raises.",{
      v:{reconciliation_id:"recon_template_example",execution_mode:"job","source_config.type":"table","source_config.table":"{{catalog}}.bronze_example.example_volume_baseline","source_config.read_mode":"batch",two_tier_verification:true,"source_config.filter_condition":"load_date = '${run_date}'","source_config.data_standardization_sql":"trim(status) AS status","source_config.hash_precomputed":false,match_keys:"example_id",compare_columns:"amount, status",transform_sql:"SELECT example_id, amount, status FROM _reconciliation_unmatched_records","error_handling.on_failure":"fail"},
      reps:{target_configs:[{target_id:"primary_product_table",target_catalog:"{{catalog}}",target_schema:"bronze_example",target_table:"example_raw_final",read_mode:"batch",hash_precomputed:true,comparison_direction:"both",append_target_table:"{{catalog}}.bronze_example.example_raw_cdc"}]}
    }],
    ["warn","Minimal · warn on failure","Baseline compared source-to-target only, failures logged and the run continues.",{
      v:{reconciliation_id:"recon_template_warn_example",execution_mode:"job","source_config.type":"table","source_config.table":"{{catalog}}.bronze_example.example_secondary_baseline","match_keys":"example_id","error_handling.on_failure":"warn"},
      reps:{target_configs:[{target_id:"secondary",target_catalog:"{{catalog}}",target_schema:"bronze_example",target_table:"example_secondary_final",comparison_direction:"source_to_target",append_target_table:"{{catalog}}.bronze_example.example_secondary_cdc"}]}
    }]
  ]
};

var PHASES={
  ing:[["Identity",["identity","dataset"]],["Source",["srctype","src_auto","src_asn1","src_zb","zip"]],["Reader",["src_common","src_norm","src_nested","retention"]],["Load strategy",["cdc"]],["Storage",["storage"]],["Security",["enc"]],["Export",["sink"]],["Quality",["dq"]],["Governance",["gov"]]],
  trn:[["Identity",["identity","dataset"]],["Inputs",["inputs","decrypt"]],["Transform",["sql"]],["Load strategy",["cdc"]],["Storage",["storage"]],["Security",["enc"]],["Export",["sink"]],["Quality",["dq"]],["Governance",["gov"]]],
  rec:[["Identity",["identity"]],["Datasets",["rsource","rtargets"]],["Matching",["matching"]],["Quality",["rdq"]]],
  root:[["Spec root",["root","tmplvars"]]],
  obs:[["Destinations",["obsmaster","obs"]],["Framework columns",["fwcols"]]]
};
function filled(x){ if(x===true||x===false) return true; return !!(x!==undefined&&x!==null&&String(x).trim()!==""); }

var STAGES=[
  "Validate spec against onboarding schema",
  "Resolve {{catalog}} / {{env}} template variables",
  "Validate governance tags exist in Unity Catalog",
  "Create missing governance tag definitions",
  "Upsert control tables",
  "Deploy Lakeflow pipeline",
  "Apply governance tags to deployed assets",
  "Run smoke check & publish telemetry"
];

function newFlow(kind,preset){
  var p=null;
  PRESETS[kind].forEach(function(x){if(x[0]===preset)p=x[3]});
  p=p||{};
  var out={v:Object.assign({},p.v||{}),kvs:JSON.parse(JSON.stringify(p.kvs||{})),reps:JSON.parse(JSON.stringify(p.reps||{}))};
  if(kind==="rec"&&!(out.reps.target_configs||[]).length) out.reps.target_configs=[{}];
  return out;
}

var DEFAULTS=null;
function defaultsFor(){
  if(DEFAULTS) return DEFAULTS;
  DEFAULTS={};
  var take=function(list){
    (list||[]).forEach(function(sec){
      (sec.fields||[]).forEach(function(f){
        if(f.d!==undefined&&DEFAULTS[f.p]===undefined) DEFAULTS[f.p]=f.d;
      });
    });
  };
  take(ING_SECTIONS()); take(TRN_SECTIONS()); take(REC_SECTIONS()); take(ROOT_SECTIONS()); take(OBS_SECTIONS());
  return DEFAULTS;
}
function repeatList(store,f){
  var list=(store||{})[f.p]||[];
  return list.length?{list:list,virtual:false}:{list:[{}],virtual:true};
}
function itemLabel(l,i,virtual){
  var base=String(l||"").split("[]").join("");
  return virtual?base:base+"["+i+"]";
}
function repeatDefaults(fields){
  var o={};
  (fields||[]).forEach(function(f){ if(f.d!==undefined&&o[f.p]===undefined) o[f.p]=f.d; });
  return o;
}


export {
  flat,
  F,
  T,
  N,
  S,
  B,
  L,
  Q,
  KV,
  REP,
  SK,
  LABEL_PREFIXES,
  shortLabel,
  CDC,
  isCdc,
  isAppendish,
  hasDeleteMarker,
  TARGET_TYPES,
  MODES,
  ENC_FIELDS,
  DEC_FIELDS,
  ROOT_SECTIONS,
  OBS_SECTIONS,
  TARGET_SECTIONS,
  ING_SECTIONS,
  TRN_SECTIONS,
  REC_SECTIONS,
  RECON_DATASET,
  PRESETS,
  PHASES,
  filled,
  STAGES,
  newFlow,
  defaultsFor,
  repeatList,
  itemLabel,
  repeatDefaults
};
