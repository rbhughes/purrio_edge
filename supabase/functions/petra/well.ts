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
    
    z_1.z          AS z_elev_kb,
    z_2.z          AS z_elev_df,
    z_3.z          AS z_elev_gr,
    z_4.z          AS z_elev_seis,
    z_5.z          AS z_td,
    z_6.z          AS z_cumoil,
    z_7.z          AS z_cumgas,
    z_8.z          AS z_cumwtr,
    z_9.z          AS z_whipstock,
    z_10.z         AS z_wtrdepth,
    z_11.z         AS z_comp_date,
    z_12.z         AS z_spud_date,
    z_13.z         AS z_permit_date,
    z_14.z         AS z_rig_date,
    z_15.z         AS z_aband_date,
    z_16.z         AS z_report_date,
    z_17.z         AS z_wrs_date,
    z_18.z         AS z_last_act_date,
    z_19.text      AS z_platform,
    z_20.z         AS z_active_datum_value,
    
    u.wsn          AS u_wsn,
    u.uwi          AS u_uwi,
    u.label        AS u_label,
    u.sortname     AS u_sortname,
    u.flags        AS u_flags,      
    
    f.name         AS f_active_datum
    
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
  LEFT OUTER JOIN zflddef f ON f.zid = 2 AND f.fid = w.elev_zid
  LEFT OUTER JOIN zdata z_20 ON w.wsn = z_20.wsn AND z_20.fid = w.elev_fid
  ${where_clause_stub}
  `;

  const order = `ORDER BY w_uwi`;

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
    LEFT OUTER JOIN zflddef f ON f.zid = 2 AND f.fid = w.elev_zid
    LEFT OUTER JOIN zdata adv ON w.wsn = adv.wsn AND adv.fid = w.elev_fid
    ${where}`;

  return {
    id_cols: idCols,
    identifier: identifier,
    order: order,
    select: select,
    where: where,
    where_clause_stub: where_clause_stub,
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
      return val.replace(/[\u0000-\u001F\u007F-\u009F]/g, "");
    } else if (type === "number") {
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
    case "excel_date":
      return (() => {
        try {
          if (obj[key].toString().match(/1[eE]\+?30/i)) {
            return null;
          }
          const d = new Date(Math.round((obj[key] - 25569) * 86400 * 1000));
          return d.toISOString();
        } catch (error) {
          console.log("ERROR", error);
          return null;
        }
      })();
    case "parse_congressional":
      return (() => {
        try {
          const b = Buffer.from(obj[key]);
          const cong = {
            township: b.subarray(4, 6).toString().split("\x00")[0],
            township_ns: b.subarray(71, 72).toString(),
            range: b.subarray(21, 23).toString().split("\x00")[0],
            range_ew: b.subarray(70, 71).toString(),
            section: b.subarray(38, 54).toString().split("\x00")[0],
            section_suffix: b.subarray(54, 70).toString().split("\x00")[0],
            meridian: b.subarray(153, 155).toString(),
            footage_ref: b.subarray(137, 152).toString().split("\x00")[0],
            spot: b.subarray(96, 136).toString().split("\x00")[0],
            footage_call_ns: b.subarray(88, 96).readDoubleLE(),
            footage_call_ns_ref: b.subarray(76, 80).readInt16LE(),
            footage_call_ew: b.subarray(80, 88).readDoubleLE(),
            footage_call_ew_ref: b.subarray(72, 76).readInt16LE(),
            remarks: b.subarray(156, 412).toString().split("\x00")[0],
          };
          return JSON.stringify(cong);
        } catch (error) {
          console.log("ERROR", error);
          return null;
        }
      })();
    case "blob_to_hex":
      return (() => {
        try {
          return Buffer.from(obj[key]).toString("hex");
        } catch (error) {
          console.log("ERROR", error);
          return null;
        }
      })();
    case "memo_to_string":
      return (() => {
        try {
          const buf = Buffer.from(obj[key], "binary");
          return ensureType("string", buf.toString("utf-8"));
        } catch (error) {
          console.log("ERROR", error);
          return null;
        }
      })();
    default:
      return ensureType(typ, obj[key]);
  }
};

const xforms = {
  // WELL

  w_wsn: {
    ts_type: "number",
  },
  w_flags: {
    ts_type: "number",
  },
  w_adddate: {
    ts_type: "number",
    xform: "excel_date",
  },
  w_chgdate: {
    ts_type: "number",
    xform: "excel_date",
  },
  w_elev_zid: {
    ts_type: "number",
  },
  w_elev_fid: {
    ts_type: "number",
  },
  w_symbol: {
    ts_type: "number",
  },
  w_uwi: {
    ts_type: "string",
  },
  w_label: {
    ts_type: "string",
  },
  w_shortname: {
    ts_type: "string",
  },
  w_wellname: {
    ts_type: "string",
  },
  w_symcode: {
    ts_type: "string",
  },
  w_operator: {
    ts_type: "string",
  },
  w_histoper: {
    ts_type: "string",
  },
  w_leasename: {
    ts_type: "string",
  },
  w_leasenumber: {
    ts_type: "string",
  },
  w_fieldname: {
    ts_type: "string",
  },
  w_fmattd: {
    ts_type: "string",
  },
  w_prodfm: {
    ts_type: "string",
  },
  w_county: {
    ts_type: "string",
  },
  w_state: {
    ts_type: "string",
  },
  w_remarks: {
    ts_type: "string",
    xform: "memo_to_string",
  },

  // LOCAT

  s_wsn: {
    ts_type: "number",
  },
  s_flags: {
    ts_type: "number",
  },
  s_x: {
    ts_type: "number",
  },
  s_y: {
    ts_type: "number",
  },
  s_z: {
    ts_type: "number",
  },
  s_lat: {
    ts_type: "number",
  },
  s_lon: {
    ts_type: "number",
  },
  s_botlat: {
    ts_type: "number",
  },
  s_botlon: {
    ts_type: "number",
  },
  s_botx: {
    ts_type: "number",
  },
  s_boty: {
    ts_type: "number",
  },
  s_congress: {
    ts_type: "number",
    xform: "parse_congressional",
  },
  s_congress_orig: {
    ts_type: "number",
    xform: "blob_to_hex",
  },
  s_texasloc: {
    ts_type: "string",
    xform: "blob_to_hex",
  },
  s_offshore: {
    ts_type: "number",
    xform: "blob_to_hex",
  },
  s_chgdate: {
    ts_type: "date",
  },

  // BHLOC

  b_wsn: {
    ts_type: "number",
  },
  b_flags: {
    ts_type: "number",
  },
  b_x: {
    ts_type: "number",
  },
  b_y: {
    ts_type: "number",
  },
  b_z: {
    ts_type: "number",
  },
  b_lat: {
    ts_type: "number",
  },
  b_lon: {
    ts_type: "number",
  },
  b_congress: {
    ts_type: "number",
    xform: "parse_congressional",
  },
  b_congress_orig: {
    ts_type: "number",
    xform: "blob_to_hex",
  },
  b_texasloc: {
    ts_type: "string",
    xform: "blob_to_hex",
  },
  b_offshore: {
    ts_type: "number",
    xform: "blob_to_hex",
  },
  b_chgdate: {
    ts_type: "date",
  },

  // ZDATA

  z_elev_kb: {
    ts_type: "number",
  },
  z_elev_df: {
    ts_type: "number",
  },
  z_elev_gr: {
    ts_type: "number",
  },
  z_elev_seis: {
    ts_type: "number",
  },
  z_td: {
    ts_type: "number",
  },
  z_cumoil: {
    ts_type: "number",
  },
  z_cumgas: {
    ts_type: "number",
  },
  z_cumwtr: {
    ts_type: "number",
  },
  z_whipstock: {
    ts_type: "number",
  },
  z_wtrdepth: {
    ts_type: "number",
  },
  z_comp_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  z_spud_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  z_permit_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  z_rig_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  z_aband_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  z_report_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  z_wrs_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  z_last_act_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  z_platform: {
    ts_type: "string",
    xform: "memo_to_string",
  },
  z_active_datum_value: {
    ts_type: "number",
  },

  // UWI

  u_wsn: {
    ts_type: "number",
  },
  u_uwi: {
    ts_type: "string",
  },
  u_label: {
    ts_type: "string",
  },
  u_sortname: {
    ts_type: "string",
  },
  u_flags: {
    ts_type: "number",
  },

  // ZFLDDEF

  f_active_datum: {
    ts_type: "string",
  },
};

const prefixes = {
  w_: "well",
  s_: "locat",
  b_: "bhloc",
  z_: "zdata",
  u_: "uwi",
  f_: "zflddef",
};

const asset_id_keys = ["w_uwi"];

const well_id_keys = ["w_uwi"];

const default_chunk = 1000;

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    asset_id_keys: asset_id_keys,
    default_chunk: default_chunk,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
