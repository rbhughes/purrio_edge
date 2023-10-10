import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn", "i.ign"];
  const idForm = idCols
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT
    w.wsn           AS w_wsn,
    w.uwi           AS w_uwi,

    i.wsn           AS i_wsn,
    i.ign           AS i_ign,
    i.flags         AS i_flags,
    i.imagefilename AS i_imagefilename,
    i.calibfilename AS i_calibfilename,

    g.ign           AS g_ign,
    g.flags         AS g_flags,
    g.groupname     AS g_groupname,
    g.desc          AS g_desc,
    g.path          AS g_path

  FROM well w
  JOIN logimage i ON w.wsn = i.wsn
  LEFT OUTER JOIN logimgrp g ON i.ign = g.ign
  ${where_clause_stub}
  `;

  const order = `ORDER BY w_uwi`;

  const identifier = `
    SELECT
      LIST(${idForm}) as keylist
    FROM well w
    JOIN logimage i ON w.wsn = i.wsn
    LEFT OUTER JOIN logimgrp g ON i.ign = g.ign
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
    case "nobody": // no match possible, but ensure switch compatibility
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

  // LOGIMAGE

  i_wsn: {
    ts_type: "number",
  },
  i_ign: {
    ts_type: "number",
  },
  i_flags: {
    ts_type: "number",
  },
  i_imagefilename: {
    ts_type: "string",
  },
  i_calibfilename: {
    ts_type: "string",
  },

  // LOGIMGRP

  g_ign: {
    ts_type: "number",
  },
  g_flags: {
    ts_type: "number",
  },
  g_groupname: {
    ts_type: "string",
  },
  g_desc: {
    ts_type: "string",
  },
  g_path: {
    ts_type: "string",
  },
};

const prefixes = {
  w_: "well",
  i_: "logimage",
  g_: "logimgrp",
};

const asset_id_keys = ["w_uwi", "i_ign"];

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
