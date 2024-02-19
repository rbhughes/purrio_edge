import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter, recency) => {
  filter = filter ? filter : "";

  const D = "|&|";
  const N = "purrNULL";
  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn", "f.mid"];
  const idForm = idCols
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

  const whereClause = ["WHERE 1=1"];

  if (recency > 0) {
    let now = Date.now() / (86400 * 1000) + 25569;
    whereClause.push(`w.chgdate >= ${now - recency} AND w.chgdate < 1E30`);
  }
  if (filter.trim().length > 0) {
    whereClause.push(filter);
  }
  const where = whereClause.join(" AND ");

  let select = `SELECT
    w.wsn          AS w_wsn,
    w.uwi          AS w_uwi,
    w.chgdate      AS w_chgdate,

    f.mid          AS f_mid,
    f.name         AS f_name,
    f.desc         AS f_desc,
    f.units        AS f_units,
    f.flags        AS f_flags,
    f.nullvalue    AS f_nullvalue,
    f.unitstype    AS f_unitstype,
    f.chgdate      AS f_chgdate,

    LIST(COALESCE(CAST(a.recid AS VARCHAR(10)),   '${N}'), '${D}') AS a_recid,
    LIST(COALESCE(CAST(a.wsn AS VARCHAR(10)),     '${N}'), '${D}') AS a_wsn,
    LIST(COALESCE(CAST(a.mid AS VARCHAR(10)),     '${N}'), '${D}') AS a_mid,
    LIST(COALESCE(CAST(a.year AS VARCHAR(10)),    '${N}'), '${D}') AS a_year,
    LIST(COALESCE(CAST(a.flags AS VARCHAR(10)),   '${N}'), '${D}') AS a_flags,
    LIST(COALESCE(CAST(a.cum AS VARCHAR(10)),     '${N}'), '${D}') AS a_cum,
    LIST(COALESCE(CAST(a.jan AS VARCHAR(10)),     '${N}'), '${D}') AS a_jan,
    LIST(COALESCE(CAST(a.feb AS VARCHAR(10)),     '${N}'), '${D}') AS a_feb,
    LIST(COALESCE(CAST(a.mar AS VARCHAR(10)),     '${N}'), '${D}') AS a_mar,
    LIST(COALESCE(CAST(a.apr AS VARCHAR(10)),     '${N}'), '${D}') AS a_apr,
    LIST(COALESCE(CAST(a.may AS VARCHAR(10)),     '${N}'), '${D}') AS a_may,
    LIST(COALESCE(CAST(a.jun AS VARCHAR(10)),     '${N}'), '${D}') AS a_jun,
    LIST(COALESCE(CAST(a.jul AS VARCHAR(10)),     '${N}'), '${D}') AS a_jul,
    LIST(COALESCE(CAST(a.aug AS VARCHAR(10)),     '${N}'), '${D}') AS a_aug,
    LIST(COALESCE(CAST(a.sep AS VARCHAR(10)),     '${N}'), '${D}') AS a_sep,
    LIST(COALESCE(CAST(a.oct AS VARCHAR(10)),     '${N}'), '${D}') AS a_oct,
    LIST(COALESCE(CAST(a.nov AS VARCHAR(10)),     '${N}'), '${D}') AS a_nov,
    LIST(COALESCE(CAST(a.dec AS VARCHAR(10)),     '${N}'), '${D}') AS a_dec,
    LIST(COALESCE(CAST(a.chgdate AS VARCHAR(10)), '${N}'), '${D}') AS a_chgdate

  FROM mopddef f
  JOIN mopddata a ON a.mid = f.mid
  JOIN well w ON a.wsn = w.wsn
  ${where_clause_stub}
  GROUP BY w.wsn, f.mid
  `;

  const order = `ORDER BY w_uwi`;

  // NOTE: key, not keylist
  const identifier = `
    SELECT
      ${idForm} AS key
    FROM mopddef f
    JOIN mopddata a ON a.mid = f.mid
    JOIN well w ON a.wsn = w.wsn
    ${where}
    GROUP BY key
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
    case "delimited_array_of_excel_dates":
      return (() => {
        const localX = (v) => {
          if (v.toString().match(/1[eE]\+?30/i)) {
            return null;
          }
          const d = new Date(Math.round((v - 25569) * 86400 * 1000));
          return d.toISOString();
        };

        try {
          return obj[key].split(D).map((v) => (v === N ? null : localX(v)));
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
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
  w_chgdate: {
    ts_type: "number",
  },

  // MOPDDEF

  f_mid: {
    ts_type: "number",
  },
  f_name: {
    ts_type: "string",
  },
  f_desc: {
    ts_type: "string",
  },
  f_units: {
    ts_type: "string",
  },
  f_flags: {
    ts_type: "number",
  },
  f_nullvalue: {
    ts_type: "number",
  },
  f_unitstype: {
    ts_type: "number",
  },
  f_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },

  // MOPDDATA

  a_recid: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_wsn: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_mid: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_year: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_flags: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_cum: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_jan: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_feb: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_mar: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_apr: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_may: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_jun: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_jul: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_aug: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_sep: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_oct: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_nov: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_dec: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_chgdate: {
    ts_type: "date",
    xform: "delimited_array_of_excel_dates",
  },
};

const prefixes = {
  w_: "well",
  f_: "mopddef",
  a_: "mopddata",
};

const asset_id_keys = ["w_uwi", "f_mid"];

const well_id_keys = ["w_uwi"];

const default_chunk = 100; // 500

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
    notes: [
      "Yes, this will be quite slow.",
      "Row changed dates are likely not implemented in CORES table; using WELL instead.",
    ],
  };
};
