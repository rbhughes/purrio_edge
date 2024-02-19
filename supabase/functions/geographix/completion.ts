import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter, recency) => {
  const D = "|&|";
  const N = "purrNULL";

  filter = filter ? filter : "";
  const whereClause = ["WHERE 1=1"];
  if (filter.trim().length > 0) {
    whereClause.push(filter);
  }
  const where = whereClause.join(" AND ");

  let whereRecent = "";
  if (recency > 0) {
    whereRecent = `WHERE row_changed_date >= DATEADD(DAY, -${recency}, CURRENT DATE)`;
  }

  let select = `SELECT * FROM (
    WITH w AS (
      SELECT
        uwi                        AS w_uwi
      FROM well
    ),
    c AS (
      SELECT
        uwi                        AS id_c_uwi,
        LIST(IFNULL(base_depth,                 '${N}', CAST(base_depth AS VARCHAR)),                '${D}' ORDER BY completion_obs_no)  AS c_base_depth,
        LIST(IFNULL(base_form,                  '${N}', CAST(base_form  AS VARCHAR)),                '${D}' ORDER BY completion_obs_no)  AS c_base_form,
        LIST(IFNULL(completion_date,            '${N}', CAST(completion_date  AS VARCHAR)),          '${D}' ORDER BY completion_obs_no)  AS c_completion_date,
        LIST(IFNULL(completion_form,            '${N}', CAST(completion_form AS VARCHAR)),           '${D}' ORDER BY completion_obs_no)  AS c_completion_form,
        LIST(IFNULL(completion_obs_no,          '${N}', CAST(completion_obs_no AS VARCHAR)),         '${D}' ORDER BY completion_obs_no)  AS c_completion_obs_no,
        LIST(IFNULL(completion_type,            '${N}', CAST(completion_type AS VARCHAR)),           '${D}' ORDER BY completion_obs_no)  AS c_completion_type,
        LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                    '${D}' ORDER BY completion_obs_no)  AS c_remark,
        LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),          '${D}' ORDER BY completion_obs_no)  AS c_row_changed_date,
        LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                    '${D}' ORDER BY completion_obs_no)  AS c_source,
        LIST(IFNULL(top_depth,                  '${N}', CAST(top_depth AS VARCHAR)),                 '${D}' ORDER BY completion_obs_no)  AS c_top_depth,
        LIST(IFNULL(top_form,                   '${N}', CAST(top_form AS VARCHAR)),                  '${D}' ORDER BY completion_obs_no)  AS c_top_form,
        LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                       '${D}' ORDER BY completion_obs_no)  AS c_uwi
      FROM well_completion
      ${whereRecent}
      GROUP BY uwi
    ) 
    SELECT
      w.*,
      c.*
    FROM w
    JOIN c ON w.w_uwi = c.id_c_uwi
    ) x`;

  const order = `ORDER BY w_uwi`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  return {
    select: select,
    count: count,
    order: order,
    where: where,
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
    default:
      return ensureType(typ, obj[key]);
  }
};

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // WELL_COMPLETION

  id_c_uwi: {
    ts_type: "string",
  },
  c_base_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_base_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_completion_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_completion_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_completion_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_completion_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_top_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_top_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
};

const prefixes = {
  w_: "well",
  c_: "well_completion",
};

const asset_id_keys = ["w_uwi"];

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
    notes: [],
  };
};
