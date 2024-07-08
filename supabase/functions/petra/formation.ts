const purr_where = "__purrWHERE__"

let select = `SELECT
  w.wsn          AS w_wsn,
  w.uwi          AS w_uwi,

  f.fid          AS f_fid,
  f.zid          AS f_zid,
  f.name         AS f_name,
  f.source       AS f_source,
  f.desc         AS f_desc,
  f.units        AS f_units,
  f.kind         AS f_kind,
  f.ndec         AS f_ndec,
  f.adddate      AS f_adddate,
  f.chgdate      AS f_chgdate,
  f.remarks      AS f_remarks,
  f.flags        AS f_flags,
  f.unitstype    AS f_unitstype,

  z.fid          AS z_fid,
  z.wsn          AS z_wsn,
  z.zid          AS z_zid,
  z.z            AS z_z,
  z.postdepth    AS z_postdepth,
  z.quality      AS z_quality,
  z.symbol       AS z_symbol,
  z.chgdate      AS z_chgdate,
  z.textlen      AS z_textlen,
  z.text         AS z_text,
  z.datalen      AS z_datalen,
  z.data         AS z_data,

  t.recid        AS t_recid,
  t.wsn          AS t_wsn,
  t.fid          AS t_fid,
  t.flags        AS t_flags,
  t.symbol       AS t_symbol,
  t.iunits       AS t_iunits,
  t.npts         AS t_npts,
  t.datasize     AS t_datasize,
  t.adddate      AS t_adddate,
  t.chgdate      AS t_chgdate,
  t.data         AS t_data,
  CAST(t.remarks AS VARCHAR(512)) AS t_remarks

FROM zflddef f
JOIN zdata z ON f.fid = z.fid
  AND f.kind = 'T'
  AND z.zid = 1
  AND z.z < 1E30
  AND z.z IS NOT NULL
LEFT OUTER JOIN zztops t ON t.wsn = z.wsn AND t.fid = z.fid
JOIN well w ON z.wsn = w.wsn 
${purr_where}
`;

const identifier_keys = ["w.wsn", "f.fid"];
const idForm = identifier_keys
  .map((i) => `CAST(${i} AS VARCHAR(10))`)
  .join(` || '-' || `);

const identifier = `
  SELECT
    LIST(${idForm}) as keylist
  FROM zflddef f
  JOIN zdata z ON f.fid = z.fid
    AND f.kind = 'T'
    AND z.zid = 1
    AND z.z < 1E30
    AND z.z IS NOT NULL
  LEFT OUTER JOIN zztops t ON t.wsn = z.wsn AND t.fid = z.fid
  JOIN well w ON z.wsn = w.wsn 
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

  // ZFLDDEF

  f_fid: {
    ts_type: "number",
  },
  f_zid: {
    ts_type: "number",
  },
  f_name: {
    ts_type: "string",
  },
  f_source: {
    ts_type: "string",
  },
  f_desc: {
    ts_type: "string",
  },
  f_units: {
    ts_type: "string",
  },
  f_kind: {
    ts_type: "string",
  },
  f_ndec: {
    ts_type: "number",
  },
  f_adddate: {
    ts_type: "date",
    xform: "excel_date",
  },
  f_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  f_remarks: {
    ts_type: "string",
    xform: "memo_to_string",
  },
  f_flags: {
    ts_type: "number",
  },
  f_unitstype: {
    ts_type: "number",
  },

  // ZDATA

  z_fid: {
    ts_type: "number",
  },
  z_wsn: {
    ts_type: "number",
  },
  z_zid: {
    ts_type: "number",
  },
  z_z: {
    ts_type: "number",
  },
  z_postdepth: {
    ts_type: "number",
  },
  z_quality: {
    ts_type: "string",
  },
  z_symbol: {
    ts_type: "number",
  },
  z_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  z_textlen: {
    ts_type: "number",
  },
  z_text: {
    ts_type: "string",
    xform: "memo_to_string",
  },
  z_datalen: {
    ts_type: "number",
  },
  z_data: {
    ts_type: "string",
    xform: "blob_to_hex",
  },

  // ZZTOPS

  t_recid: {
    ts_type: "number",
  },
  t_wsn: {
    ts_type: "number",
  },
  t_fid: {
    ts_type: "number",
  },
  t_flags: {
    ts_type: "number",
  },
  t_symbol: {
    ts_type: "number",
  },
  t_iunits: {
    ts_type: "number",
  },
  t_npts: {
    ts_type: "number",
  },
  t_datasize: {
    ts_type: "number",
  },
  t_adddate: {
    ts_type: "date",
    xform: "excel_date",
  },
  t_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  t_data: {
    ts_type: "string", // actually json
    xform: "parse_zztops",
  },
  t_remarks: {
    ts_type: "string",
  },
};

const asset_id_keys = ["w_uwi", "f_fid"];

const default_chunk = 100; // 1000

const notes = [];

const order = `ORDER BY w.uwi`;

const prefixes = {
  w_: "well",
  f_: "zflddef",
  z_: "zdata",
  t_: "zztops",
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
    //post_process,
    prefixes,
    //purr_delimiter,
    //purr_null,
    purr_where,
    select,
    well_id_keys,
    xforms,
  };
};

