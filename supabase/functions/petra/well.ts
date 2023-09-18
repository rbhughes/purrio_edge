import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn"];
  const idForm = idCols
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT
    w.wsn          AS w_wsn,
    w.flags        AS w_flags,
    w.adddate      AS w_adddate,
    w.chgdate      AS w_chgdate,
    w.elev_zid     AS w_elev_zid,
    w.elev_fid     AS w_elev_fid,
    w.symbol       AS w_symbol,
    w.uwi          AS w_uwi,
    w.label        AS w_label,
    w.shortname    AS w_shortname,
    w.wellname     AS w_wellname,
    w.symcode      AS w_symcode,
    w.operator     AS w_operator,
    w.histoper     AS w_histoper,
    w.leasename    AS w_leasename,
    w.leasenumber  AS w_leasenumber,
    w.fieldname    AS w_fieldname,
    w.fmattd       AS w_fmattd,
    w.prodfm       AS w_prodfm,
    w.county       AS w_county,
    w.state        AS w_state,
    w.remarks      AS w_remarks,
    
    s.wsn          AS s_wsn,
    s.flags        AS s_flags,
    s.x            AS s_x,
    s.y            AS s_y,
    s.z            AS s_z,
    s.lat          AS s_lat,
    s.lon          AS s_lon,
    s.botlat       AS s_botlat,
    s.botlon       AS s_botlon,
    s.botx         AS s_botx,
    s.boty         AS s_boty,
    s.congress     AS s_congress,
    s.congress     AS s_congress_orig,
    s.texasloc     AS s_texasloc,
    s.offshore     AS s_offshore,
    
    b.wsn          AS b_wsn,
    b.flags        AS b_flags,
    b.x            AS b_x,
    b.y            AS b_y,
    b.z            AS b_z,
    b.lat          AS b_lat,
    b.lon          AS b_lon,
    b.congress     AS b_congress,
    b.congress     AS b_congress_orig,
    b.texasloc     AS b_texasloc,
    b.offshore     AS b_offshore,
    b.chgdate      AS b_chgdate,
    
    z_1.z          AS w_elev_kb,
    z_2.z          AS w_elev_df,
    z_3.z          AS w_elev_gr,
    z_4.z          AS w_elev_seis,
    z_5.z          AS w_td,
    z_6.z          AS w_cumoil,
    z_7.z          AS w_cumgas,
    z_8.z          AS w_cumwtr,
    z_9.z          AS w_whipstock,
    z_10.z         AS w_wtrdepth,
    z_11.z         AS w_comp_date,
    z_12.z         AS w_spud_date,
    z_13.z         AS w_permit_date,
    z_14.z         AS w_rig_date,
    z_15.z         AS w_aband_date,
    z_16.z         AS w_report_date,
    z_17.z         AS w_wrs_date,
    z_18.z         AS w_last_act_date,
    z_19.text      AS w_platform,
    
    u.wsn          AS u_wsn,
    u.uwi          AS u_uwi,
    u.label        AS u_label,
    u.sortname     AS u_sortname,
    u.flags        AS u_flags,      
    
    zf.name        AS o_active_datum,
    adv.z          AS o_active_datum_value
    
  FROM well w
  LEFT JOIN locat s ON s.wsn = w.wsn
  LEFT JOIN bhloc b ON b.wsn = w.wsn
  LEFT JOIN uwi u ON u.wsn = w.wsn
  LEFT OUTER JOIN zdata z_1 ON w.wsn = z_1.wsn AND z_1.fid = 1
  LEFT OUTER JOIN zdata z_2 ON w.wsn = z_2.wsn AND z_2.fid = 2
  LEFT OUTER JOIN zdata z_3 ON w.wsn = z_3.wsn AND z_3.fid = 3
  LEFT OUTER JOIN zdata z_4 ON w.wsn = z_4.wsn AND z_4.fid = 4
  LEFT OUTER JOIN zdata z_5 ON w.wsn = z_5.wsn AND z_5.fid = 5
  LEFT OUTER JOIN zdata z_6 ON w.wsn = z_6.wsn AND z_6.fid = 6
  LEFT OUTER JOIN zdata z_7 ON w.wsn = z_7.wsn AND z_7.fid = 7
  LEFT OUTER JOIN zdata z_8 ON w.wsn = z_8.wsn AND z_8.fid = 8
  LEFT OUTER JOIN zdata z_9 ON w.wsn = z_9.wsn AND z_9.fid = 9
  LEFT OUTER JOIN zdata z_10 ON w.wsn = z_10.wsn AND z_10.fid = 10
  LEFT OUTER JOIN zdata z_11 ON w.wsn = z_11.wsn AND z_11.fid = 11
  LEFT OUTER JOIN zdata z_12 ON w.wsn = z_12.wsn AND z_12.fid = 12
  LEFT OUTER JOIN zdata z_13 ON w.wsn = z_13.wsn AND z_13.fid = 13
  LEFT OUTER JOIN zdata z_14 ON w.wsn = z_14.wsn AND z_14.fid = 14
  LEFT OUTER JOIN zdata z_15 ON w.wsn = z_15.wsn AND z_15.fid = 15
  LEFT OUTER JOIN zdata z_16 ON w.wsn = z_16.wsn AND z_16.fid = 16
  LEFT OUTER JOIN zdata z_17 ON w.wsn = z_17.wsn AND z_17.fid = 17
  LEFT OUTER JOIN zdata z_18 ON w.wsn = z_18.wsn AND z_18.fid = 18
  LEFT OUTER JOIN zdata z_19 ON w.wsn = z_19.wsn AND z_19.fid = 19
  LEFT OUTER JOIN zflddef zf ON zf.zid = 2 AND zf.fid = w.elev_zid
  LEFT OUTER JOIN zdata adv ON w.wsn = adv.wsn AND adv.fid = w.elev_fid
  ${where_clause_stub}
  `;

  const order = `ORDER BY w_uwi`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  //const fast_count = `SELECT COUNT(DISTINCT uwi) AS count FROM well`;

  const identifier = `
    SELECT
      LIST(${idForm}) as keylist
    FROM well w
    LEFT JOIN locat s ON s.wsn = w.wsn
    LEFT JOIN bhloc b ON b.wsn = w.wsn
    LEFT JOIN uwi u ON u.wsn = w.wsn
    LEFT OUTER JOIN zdata z_1 ON w.wsn = z_1.wsn AND z_1.fid = 1
    LEFT OUTER JOIN zdata z_2 ON w.wsn = z_2.wsn AND z_2.fid = 2
    LEFT OUTER JOIN zdata z_3 ON w.wsn = z_3.wsn AND z_3.fid = 3
    LEFT OUTER JOIN zdata z_4 ON w.wsn = z_4.wsn AND z_4.fid = 4
    LEFT OUTER JOIN zdata z_5 ON w.wsn = z_5.wsn AND z_5.fid = 5
    LEFT OUTER JOIN zdata z_6 ON w.wsn = z_6.wsn AND z_6.fid = 6
    LEFT OUTER JOIN zdata z_7 ON w.wsn = z_7.wsn AND z_7.fid = 7
    LEFT OUTER JOIN zdata z_8 ON w.wsn = z_8.wsn AND z_8.fid = 8
    LEFT OUTER JOIN zdata z_9 ON w.wsn = z_9.wsn AND z_9.fid = 9
    LEFT OUTER JOIN zdata z_10 ON w.wsn = z_10.wsn AND z_10.fid = 10
    LEFT OUTER JOIN zdata z_11 ON w.wsn = z_11.wsn AND z_11.fid = 11
    LEFT OUTER JOIN zdata z_12 ON w.wsn = z_12.wsn AND z_12.fid = 12
    LEFT OUTER JOIN zdata z_13 ON w.wsn = z_13.wsn AND z_13.fid = 13
    LEFT OUTER JOIN zdata z_14 ON w.wsn = z_14.wsn AND z_14.fid = 14
    LEFT OUTER JOIN zdata z_15 ON w.wsn = z_15.wsn AND z_15.fid = 15
    LEFT OUTER JOIN zdata z_16 ON w.wsn = z_16.wsn AND z_16.fid = 16
    LEFT OUTER JOIN zdata z_17 ON w.wsn = z_17.wsn AND z_17.fid = 17
    LEFT OUTER JOIN zdata z_18 ON w.wsn = z_18.wsn AND z_18.fid = 18
    LEFT OUTER JOIN zdata z_19 ON w.wsn = z_19.wsn AND z_19.fid = 19
    LEFT OUTER JOIN zflddef zf ON zf.zid = 2 AND zf.fid = w.elev_zid
    LEFT OUTER JOIN zdata adv ON w.wsn = adv.wsn AND adv.fid = w.elev_fid
    ${where}`;

  return {
    identifier: identifier,
    id_cols: idCols,
    where_clause_stub: where_clause_stub,
    select: select,
    count: count,
    order: order,
    where: where,
  };
};

const xformer = (args) => {
  let { func, key, typ, arg, obj } = args;

  const ensureType = (type: string, val: any) => {
    if (val == null) {
      return null;
    } else if (type === "object") {
      console.log("UNEXPECTED OBJECT TYPE! (needs xformer)", type);
      console.log(val);
      return null;
    } else if (type === "string") {
      //return decodeWin1252(val)
      return val.replace(/[\u0000-\u001F\u007F-\u009F]/g, "");
    } else if (type === "number") {
      // cuz blank strings (\t\r\n) evaluate to 0
      if (val.toString().replace(/\s/g, "") === "") {
        return null;
      }
      let n = Number(val);
      return isNaN(n) ? null : n;
    } else if (type === "date") {
      try {
        return new Date(val).toISOString();
      } catch (error) {
        return null;
      }
    } else {
      console.log("ENSURE TYPE SOMETHING ELSE (xformer)", type);
      return "XFORM ME";
    }
  };

  if (obj[key] == null) {
    return null;
  }

  switch (func) {
    case "blob_to_hex":
      return (() => {
        try {
          return Buffer.from(obj[key]).toString("hex");
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    default:
      return ensureType(typ, obj[key]);
  }
};

const xforms = {};

const prefixes = {
  w_: "well",
  co_: "legal_congress_loc",
};

const global_id_keys = ["w_uwi"];

const well_id_keys = ["w_uwi"];

const pg_cols = ["id", "repo_id", "well_id", "geo_type", "tag", "doc"];

const default_chunk = 1000;

///////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    default_chunk: default_chunk,
    global_id_keys: global_id_keys,
    pg_cols: pg_cols,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
