const purr_where = "__purrWHERE__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

  let select = `SELECT
    w.wsn          AS w_wsn,
    w.uwi          AS w_uwi,
    w.chgdate      AS w_chgdate,
    LIST(COALESCE(CAST(c.recid AS VARCHAR(10)),    '${purr_null}'), '${purr_delimiter}') AS c_recid,
    LIST(COALESCE(CAST(c.wsn AS VARCHAR(10)),      '${purr_null}'), '${purr_delimiter}') AS c_wsn,
    LIST(COALESCE(CAST(c.flags AS VARCHAR(10)),    '${purr_null}'), '${purr_delimiter}') AS c_flags,
    LIST(COALESCE(CAST(c.lithcode AS VARCHAR(10)), '${purr_null}'), '${purr_delimiter}') AS c_lithcode,
    LIST(COALESCE(CAST(c.date AS VARCHAR(10)),     '${purr_null}'), '${purr_delimiter}') AS c_date,
    LIST(COALESCE(CAST(c.top AS VARCHAR(10)),      '${purr_null}'), '${purr_delimiter}') AS c_top,
    LIST(COALESCE(CAST(c.base AS VARCHAR(10)),     '${purr_null}'), '${purr_delimiter}') AS c_base,
    LIST(COALESCE(CAST(c.recover AS VARCHAR(10)),  '${purr_null}'), '${purr_delimiter}') AS c_recover,
    LIST(COALESCE(c.type,                          '${purr_null}'), '${purr_delimiter}') AS c_type,
    LIST(COALESCE(c.qual,                          '${purr_null}'), '${purr_delimiter}') AS c_qual,
    LIST(COALESCE(c.fmname,                        '${purr_null}'), '${purr_delimiter}') AS c_fmname,
    LIST(COALESCE(c.desc,                          '${purr_null}'), '${purr_delimiter}') AS c_desc,
    LIST(COALESCE(CAST(c.remark AS VARCHAR(512)),  '${purr_null}'), '${purr_delimiter}') AS c_remark
  FROM well w
  JOIN cores c ON c.wsn = w.wsn
  ${purr_where}
  GROUP BY w.wsn
  `;

const identifier_keys = ["w.wsn"];
const idForm = identifier_keys
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

// NOTE: key, not keylist
// const identifier = `
//   SELECT
//     DISTINCT(c.wsn) AS key
//   FROM well w
//   JOIN cores c ON w.wsn = c.wsn
//   ${purr_where}
//   `;

const identifier = `
  SELECT
    DISTINCT(${idForm}) AS key
  FROM well w
  JOIN cores c ON w.wsn = c.wsn
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
  w_chgdate: {
    ts_type: "number",
    xform: "excel_date"
  },

  // CORES

  c_recid: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_wsn: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_flags: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_lithcode: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_date: {
    ts_type: "date",
    xform: "delimited_array_of_excel_dates",
  },
  c_top: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_base: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_recover: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_qual: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_fmname: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_remark: {
    ts_type: "string",
    xform: "delimited_array_of_memo",
  },
};

const asset_id_keys = ["w_uwi"];

const default_chunk = 100; 

const notes = [];

const order = `ORDER BY w.uwi`;

const prefixes = {
  w_: "well",
  c_: "cores",
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
    prefixes,
    purr_delimiter,
    purr_null,
    purr_where,
    select,
    well_id_keys,
    xforms,
  };
};
