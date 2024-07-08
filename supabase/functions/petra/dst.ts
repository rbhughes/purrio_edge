const purr_where = "__purrWHERE__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

let select = `SELECT
  w.wsn          AS w_wsn,
  w.uwi          AS w_uwi,

  f.recid        AS f_recid,
  f.wsn          AS f_wsn,
  f.numrecov     AS f_numrecov,
  f.nummts       AS f_nummts,
  f.flags        AS f_flags,
  f.date         AS f_date,
  f.top          AS f_top,
  f.base         AS f_base,
  f.ihp          AS f_ihp,
  f.fhp          AS f_fhp,
  f.ffp          AS f_ffp,
  f.isp          AS f_isp,
  f.fsp          AS f_fsp,
  f.bht          AS f_bht,
  f.bhp          AS f_bhp,
  f.choke        AS f_choke,
  f.cushamt      AS f_cushamt,
  f.testtype     AS f_testtype,
  f.fmname       AS f_fmname,
  f.cushtype     AS f_cushtype,
  f.ohtime       AS f_ohtime,
  f.sitime       AS f_sitime,
  f.remark       AS f_remark,
  f.recov        AS f_recov,
  f.mts          AS f_mts,
  f.chgdate      AS f_chgdate,
  f.unitstype    AS f_unitstype

FROM well w
JOIN fmtest f ON f.wsn = w.wsn AND f.testtype = 'D'
${purr_where}
`;

const identifier_keys = ["w.wsn", "f.recid"];
const idForm = identifier_keys
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

const identifier = `
  SELECT
    LIST(${idForm}) AS keylist
  FROM well w
  JOIN fmtest f ON f.wsn = w.wsn AND f.testtype = 'D'
  ${purr_where}
  `;

const xforms = {
  // WELL

  w_wsn: {
    ts_type: "number",
  },
  w_uwi: {
    ts_type: "string",
  },

  // FMTEST

  f_recid: {
    ts_type: "number",
  },
  f_wsn: {
    ts_type: "number",
  },
  f_numrecov: {
    ts_type: "number",
  },
  f_nummts: {
    ts_type: "number",
  },
  f_flags: {
    ts_type: "number",
  },
  f_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  f_top: {
    ts_type: "number",
  },
  f_base: {
    ts_type: "number",
  },

  f_ihp: {
    ts_type: "number",
  },
  f_fhp: {
    ts_type: "number",
  },
  f_ifp: {
    ts_type: "number",
  },
  f_ffp: {
    ts_type: "number",
  },
  f_isp: {
    ts_type: "number",
  },
  f_fsp: {
    ts_type: "number",
  },
  f_bht: {
    ts_type: "number",
  },
  f_bhp: {
    ts_type: "number",
  },
  f_choke: {
    ts_type: "number",
  },
  f_cushamt: {
    ts_type: "number",
  },
  f_testtype: {
    ts_type: "string",
  },
  f_fmname: {
    ts_type: "string",
  },
  f_cushtype: {
    ts_type: "string",
  },
  f_ohtime: {
    ts_type: "string",
  },
  f_sitime: {
    ts_type: "string",
  },
  f_remark: {
    ts_type: "number",
    xform: "memo_to_string",
  },
  f_recov: {
    ts_type: "string",
    xform: "fmtest_recovery",
  },
  // TODO: decipher what mts BLOB contains, if anything
  f_mts: {
    ts_type: "string",
    xform: "memo_to_string",
  },
  f_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  f_unitstype: {
    ts_type: "number",
  },
};

const asset_id_keys = ["w_uwi", "f_recid"];

const default_chunk = 100;

const notes = [];

const order = `ORDER BY w.uwi`;

const post_process = ['aggregate_fmtest']

const prefixes = {
  w_: "well",
  f_: "fmtest",
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
    purr_delimiter,
    purr_null,
    purr_where,
    select,
    well_id_keys,
    xforms,
  };
};
