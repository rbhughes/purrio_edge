const purr_where = "__purrWHERE__"

let select = `SELECT
  w.wsn          AS w_wsn,
  w.uwi          AS w_uwi,

  p.recid        AS p_recid,
  p.wsn          AS p_wsn,
  p.numtreat     AS p_numtreat,
  p.flags        AS p_flags,
  p.date         AS p_date,
  p.top          AS p_top,
  p.base         AS p_base,
  p.oilvol       AS p_oilvol,
  p.gasvol       AS p_gasvol,
  p.wtrvol       AS p_wtrvol,
  p.ftp          AS p_ftp,
  p.fcp          AS p_fcp,
  p.stp          AS p_stp,
  p.scp          AS p_scp,
  p.bht          AS p_bht,
  p.bhp          AS p_bhp,
  p.choke        AS p_choke,
  p.duration     AS p_duration,
  p.caof         AS p_caof,
  p.oilgty       AS p_oilgty,
  p.gasgty       AS p_gasgty,
  p.gor          AS p_gor,
  p.testtype     AS p_testtype,
  p.fmname       AS p_fmname,
  p.oilunit      AS p_oilunit,
  p.gasunit      AS p_gasunit,
  p.wtrunit      AS p_wtrunit,
  p.remark       AS p_remark,
  p.treat        AS p_treat,
  p.chgdate      AS p_chgdate,
  p.unitstype    AS p_unitstype

FROM well w
JOIN pdtest p ON p.wsn = w.wsn
${purr_where}
`;

const identifier_keys = ["w.wsn", "p.recid"];
const idForm = identifier_keys
  .map((i) => `CAST(${i} AS VARCHAR(10))`)
  .join(` || '-' || `);

const identifier = `
  SELECT
    LIST(${idForm}) AS keylist
  FROM well w
  JOIN pdtest p ON p.wsn = w.wsn
  ${purr_where}`;

const xforms = {
  // WELL

  w_wsn: {
    ts_type: "number",
  },
  w_uwi: {
    ts_type: "string",
  },

  // PDTEST

  p_recid: {
    ts_type: "number",
  },
  p_wsn: {
    ts_type: "number",
  },
  p_numtreat: {
    ts_type: "number",
  },
  p_flags: {
    ts_type: "number",
  },
  p_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  p_top: {
    ts_type: "number",
  },
  p_base: {
    ts_type: "number",
  },
  p_oilvol: {
    ts_type: "number",
  },
  p_gasvol: {
    ts_type: "number",
  },
  p_wtrvol: {
    ts_type: "number",
  },
  p_ftp: {
    ts_type: "number",
  },
  p_fcp: {
    ts_type: "number",
  },
  p_stp: {
    ts_type: "number",
  },
  p_scp: {
    ts_type: "number",
  },
  p_bht: {
    ts_type: "number",
  },
  p_bhp: {
    ts_type: "number",
  },
  p_choke: {
    ts_type: "number",
  },
  p_duration: {
    ts_type: "number",
  },
  p_caof: {
    ts_type: "number",
  },
  p_oilgty: {
    ts_type: "number",
  },
  p_gasgty: {
    ts_type: "number",
  },
  p_gor: {
    ts_type: "number",
  },
  p_testtype: {
    ts_type: "string",
  },
  p_fmname: {
    ts_type: "string",
  },
  p_oilunit: {
    ts_type: "string",
  },
  p_gasunit: {
    ts_type: "string",
  },
  p_wtrunit: {
    ts_type: "string",
  },
  p_remark: {
    ts_type: "number",
    xform: "memo_to_string",
  },
  p_treat: {
    ts_type: "string",
    xform: "pdtest_treatment",
  },
  p_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  p_unitstype: {
    ts_type: "number",
  },
};

const asset_id_keys = ["w_uwi", "p_recid"];

const default_chunk = 100;

const notes = [];

const order = `ORDER BY w.uwi`;

const post_process = ['aggregate_pdtest']

const prefixes = {
  w_: "well",
  p_: "pdtest",
};

const well_id_keys = ["w_uwi"];

export const getAssetDNA = () => {
  return {
    asset_id_keys,
    default_chunk,
    identifier,
    identifier_keys,
    notes,
    order,
    post_process,
    prefixes,
    purr_where,
    select,
    well_id_keys,
    xforms,
  };
};
