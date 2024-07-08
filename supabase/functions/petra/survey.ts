const purr_where = "__purrWHERE__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

let select = `SELECT
  w.wsn          AS w_wsn,
  w.uwi          AS w_uwi,

  d.survrecid    AS d_survrecid,
  d.wsn          AS d_wsn,
  d.flags        AS d_flags,
  d.datasize     AS d_datasize,
  d.active       AS d_active,
  d.adddate      AS d_adddate,
  d.chgdate      AS d_chgdate,
  d.numrecs      AS d_numrecs,
  d.md1          AS d_md1,
  d.md2          AS d_md2,
  d.tvd1         AS d_tvd1,
  d.tvd2         AS d_tvd2,
  d.xoff1        AS d_xoff1,
  d.xoff2        AS d_xoff2,
  d.yoff1        AS d_yoff1,
  d.yoff2        AS d_yoff2,
  d.xyunits      AS d_xyunits,
  d.depunits     AS d_depunits,
  d.dippresent   AS d_dippresent,
  d.remarks      AS d_remarks,
  d.vs_1         AS d_vs_1,
  d.vs_2         AS d_vs_2,
  d.vs_3         AS d_vs_3,
  d.data         AS d_data,

  f.name         AS f_survey_type,

  LIST(COALESCE(CAST(v.wsn AS VARCHAR(10)),      '${purr_null}'), '${purr_delimiter}') AS v_wsn,
  LIST(COALESCE(CAST(v.md/100 AS VARCHAR(10)),   '${purr_null}'), '${purr_delimiter}') AS v_md,
  LIST(COALESCE(CAST(v.tvd AS VARCHAR(20)),      '${purr_null}'), '${purr_delimiter}') AS v_tvd,
  LIST(COALESCE(CAST(v.xoff AS VARCHAR(20)),     '${purr_null}'), '${purr_delimiter}') AS v_xoff,
  LIST(COALESCE(CAST(v.yoff AS VARCHAR(20)),     '${purr_null}'), '${purr_delimiter}') AS v_yoff,
  LIST(COALESCE(CAST(v.dip AS VARCHAR(20)),      '${purr_null}'), '${purr_delimiter}') AS v_dip,
  LIST(COALESCE(CAST(v.azm AS VARCHAR(20)),      '${purr_null}'), '${purr_delimiter}') AS v_azm,
  LIST(COALESCE(CAST(v.vsection AS VARCHAR(20)), '${purr_null}'), '${purr_delimiter}') AS v_vsection,
  LIST(COALESCE(CAST(v.d1 AS VARCHAR(20)),       '${purr_null}'), '${purr_delimiter}') AS v_d1,
  LIST(COALESCE(CAST(v.d2 AS VARCHAR(20)),       '${purr_null}'), '${purr_delimiter}') AS v_d2,
  LIST(COALESCE(CAST(v.d3 AS VARCHAR(20)),       '${purr_null}'), '${purr_delimiter}') AS v_d3

FROM well w
LEFT OUTER JOIN dirsurv v ON v.wsn = w.wsn
JOIN dirsurvdata d ON d.wsn = w.wsn
JOIN dirsurvdef f ON f.survrecid = d.survrecid
${purr_where}
GROUP BY w.wsn
`;

const identifier_keys = ["w.wsn"];
const idForm = identifier_keys
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

const identifier = `
  SELECT
    ${idForm} AS key
  FROM well w
  JOIN dirsurvdata d ON d.wsn = w.wsn
  JOIN dirsurvdef f ON f.survrecid = d.survrecid
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

  // DIRSURVDATA

  d_survrecid: {
    ts_type: "number",
  },
  d_wsn: {
    ts_type: "number",
  },
  d_flags: {
    ts_type: "number",
  },
  d_datasize: {
    ts_type: "number",
  },
  d_active: {
    ts_type: "number",
  },
  d_adddate: {
    ts_type: "date",
    xform: "excel_date",
  },
  d_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  d_numrecs: {
    ts_type: "number",
  },
  d_md1: {
    ts_type: "number",
  },
  d_md2: {
    ts_type: "number",
  },
  d_tvd1: {
    ts_type: "number",
  },
  d_tvd2: {
    ts_type: "number",
  },
  d_xoff1: {
    ts_type: "number",
  },
  d_xoff2: {
    ts_type: "number",
  },
  d_yoff1: {
    ts_type: "number",
  },
  d_yoff2: {
    ts_type: "number",
  },
  d_xyunits: {
    ts_type: "number",
  },
  d_depunits: {
    ts_type: "number",
  },
  d_dippresent: {
    ts_type: "number",
  },
  d_remarks: {
    ts_type: "string",
  },
  d_vs_1: {
    ts_type: "number",
  },
  d_vs_2: {
    ts_type: "number",
  },
  d_vs_3: {
    ts_type: "number",
  },

  d_data: {
    ts_type: "string",
    xform: "blob_to_hex",
  },

  // DIRSURVDEF

  f_survey_type: {
    ts_type: "string",
  },

  // DIRSURV

  v_wsn: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_md: {
    ts_type: "number",
    //xform: "delimited_array_drop_two_decimal",
    xform: "delimited_array_with_nulls",
  },
  v_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_xoff: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_yoff: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_dip: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_azm: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_vsection: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_d1: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_d2: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_d3: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
};

const asset_id_keys = ["w_uwi"];

const default_chunk = 100;

const notes = [
  "Not sure why depths in dirsurv.md are 100x off. Note the divisor."
];

const order = `ORDER BY w.uwi`;

const prefixes = {
  w_: "well",
  d_: "dirsurvdata",
  f_: "dirsurvdef",
  v_: "dirsurv",
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

/*import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

// assumes one survey per well (dirsurvdata.survrecid = 1 is always true?)

// Need to parse the survey blob? Here's how in Python:
// # dirsurvdata = 8-byte doubles, unpack as 64-bit float
// for i in range(0, length, 56):
//     md.append(struct.unpack('d',       ba[i: i+8])[0]/100)
//     tvd.append(struct.unpack('d',      ba[i+8: i+16])[0])
//     x_offset.append(struct.unpack('d', ba[i+16: i+24])[0])
//     y_offset.append(struct.unpack('d', ba[i+24: i+32])[0])
//     dip.append(struct.unpack('d',      ba[i+32: i+40])[0])
//     azimuth.append(struct.unpack('d',  ba[i+40: i+48])[0])

const defineSQL = (filter, recency) => {
  filter = filter ? filter : "";

  const D = "|&|";
  const N = "purrNULL";
  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn"];
  const idForm = idCols
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

  const whereClause = ["WHERE 1=1"];

  if (recency > 0) {
    let now = Date.now() / (86400 * 1000) + 25569;
    whereClause.push(`d.chgdate >= ${now - recency} AND d.chgdate < 1E30`);
  }
  if (filter.trim().length > 0) {
    whereClause.push(filter);
  }
  const where = whereClause.join(" AND ");

  let select = `SELECT
    w.wsn          AS w_wsn,
    w.uwi          AS w_uwi,

    d.survrecid    AS d_survrecid,
    d.wsn          AS d_wsn,
    d.flags        AS d_flags,
    d.datasize     AS d_datasize,
    d.active       AS d_active,
    d.adddate      AS d_adddate,
    d.chgdate      AS d_chgdate,
    d.numrecs      AS d_numrecs,
    d.md1          AS d_md1,
    d.md2          AS d_md2,
    d.tvd1         AS d_tvd1,
    d.tvd2         AS d_tvd2,
    d.xoff1        AS d_xoff1,
    d.xoff2        AS d_xoff2,
    d.yoff1        AS d_yoff1,
    d.yoff2        AS d_yoff2,
    d.xyunits      AS d_xyunits,
    d.depunits     AS d_depunits,
    d.dippresent   AS d_dippresent,
    d.remarks      AS d_remarks,
    d.vs_1         AS d_vs_1,
    d.vs_2         AS d_vs_2,
    d.vs_3         AS d_vs_3,
    d.data         AS d_data,

    f.name         AS f_survey_type,

    LIST(COALESCE(CAST(v.wsn AS VARCHAR(10)),      '${purr_null}'), '${purr_delimiter}') AS v_wsn,
    LIST(COALESCE(CAST(v.md AS VARCHAR(10)),       '${purr_null}'), '${purr_delimiter}') AS v_md,
    LIST(COALESCE(CAST(v.tvd AS VARCHAR(20)),      '${purr_null}'), '${purr_delimiter}') AS v_tvd,
    LIST(COALESCE(CAST(v.xoff AS VARCHAR(20)),     '${purr_null}'), '${purr_delimiter}') AS v_xoff,
    LIST(COALESCE(CAST(v.yoff AS VARCHAR(20)),     '${purr_null}'), '${purr_delimiter}') AS v_yoff,
    LIST(COALESCE(CAST(v.dip AS VARCHAR(20)),      '${purr_null}'), '${purr_delimiter}') AS v_dip,
    LIST(COALESCE(CAST(v.azm AS VARCHAR(20)),      '${purr_null}'), '${purr_delimiter}') AS v_azm,
    LIST(COALESCE(CAST(v.vsection AS VARCHAR(20)), '${purr_null}'), '${purr_delimiter}') AS v_vsection,
    LIST(COALESCE(CAST(v.d1 AS VARCHAR(20)),       '${purr_null}'), '${purr_delimiter}') AS v_d1,
    LIST(COALESCE(CAST(v.d2 AS VARCHAR(20)),       '${purr_null}'), '${purr_delimiter}') AS v_d2,
    LIST(COALESCE(CAST(v.d3 AS VARCHAR(20)),       '${purr_null}'), '${purr_delimiter}') AS v_d3

  FROM well w
  LEFT OUTER JOIN dirsurv v ON v.wsn = w.wsn
  JOIN dirsurvdata d ON d.wsn = w.wsn
  JOIN dirsurvdef f ON f.survrecid = d.survrecid
  ${where_clause_stub}
  GROUP BY w.wsn
  `;

  const order = `ORDER BY w_uwi`;

  // NOTE: key, not keylist
  const identifier = `
    SELECT
      ${idForm} AS key
    FROM well w
    JOIN dirsurvdata d ON d.wsn = w.wsn
    JOIN dirsurvdef f ON f.survrecid = d.survrecid
    ${where}
    `;

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
  const D = "|&|";
  const N = "purrNULL";

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
    case "delimited_array_drop_two_decimal":
      // no idea why md values are 100x off in Petra here
      return (() => {
        try {
          return obj[key]
            .split(D)
            .map((v) => (v === N ? null : ensureType(typ, v / 100)));
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    case "delimited_array_with_nulls":
      return (() => {
        try {
          return obj[key]
            .split(D)
            .map((v) => (v === N ? null : ensureType(typ, v)));
        } catch (error) {
          console.log("ERROR", error);
          return;
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
    default:
      return ensureType(typ, obj[key]);
  }
};

const xforms = {
  // WELL

  w_wsn: {
    ts_type: "number",
  },
  w_uwi: {
    ts_type: "string",
  },

  // DIRSURVDATA

  d_survrecid: {
    ts_type: "number",
  },
  d_wsn: {
    ts_type: "number",
  },
  d_flags: {
    ts_type: "number",
  },
  d_datasize: {
    ts_type: "number",
  },
  d_active: {
    ts_type: "number",
  },
  d_adddate: {
    ts_type: "date",
    xform: "excel_date",
  },
  d_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  d_numrecs: {
    ts_type: "number",
  },
  d_md1: {
    ts_type: "number",
  },
  d_md2: {
    ts_type: "number",
  },
  d_tvd1: {
    ts_type: "number",
  },
  d_tvd2: {
    ts_type: "number",
  },
  d_xoff1: {
    ts_type: "number",
  },
  d_xoff2: {
    ts_type: "number",
  },
  d_yoff1: {
    ts_type: "number",
  },
  d_yoff2: {
    ts_type: "number",
  },
  d_xyunits: {
    ts_type: "number",
  },
  d_depunits: {
    ts_type: "number",
  },
  d_dippresent: {
    ts_type: "number",
  },
  d_remarks: {
    ts_type: "string",
  },
  d_vs_1: {
    ts_type: "number",
  },
  d_vs_2: {
    ts_type: "number",
  },
  d_vs_3: {
    ts_type: "number",
  },

  d_data: {
    ts_type: "string",
    xform: "blob_to_hex",
  },

  // DIRSURVDEF

  f_survey_type: {
    ts_type: "string",
  },

  // DIRSURV

  v_wsn: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_md: {
    ts_type: "number",
    xform: "delimited_array_drop_two_decimal",
  },
  v_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_xoff: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_yoff: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_dip: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_azm: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_vsection: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_d1: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_d2: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_d3: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
};

const prefixes = {
  w_: "well",
  d_: "dirsurvdata",
  f_: "dirsurvdef",
  v_: "dirsurv",
};

const asset_id_keys = ["w_uwi"];

const well_id_keys = ["w_uwi"];

const default_chunk = 100; // 1000

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter, recency) => {
  return {
    asset_id_keys: asset_id_keys,
    default_chunk: default_chunk,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter, recency),
    well_id_keys: well_id_keys,
    xforms: xforms,
    notes: [],
  };
};
*/