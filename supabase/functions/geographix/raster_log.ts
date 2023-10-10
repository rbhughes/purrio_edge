import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT * FROM (
    WITH w AS (
      SELECT
        uwi                          AS w_uwi
      FROM well
    ),
    v AS (
      SELECT
        bfile_data                   AS v_bfile_data,
        blk_no                       AS v_blk_no,
        blob_data                    AS v_blob_data,
        blob_data                    AS v_blob_data_orig,
        bytes_used                   AS v_bytes_used,
        datatype                     AS v_datatype,
        ow_rel_path                  AS v_ow_rel_path,
        vec_storage_type             AS v_vec_storage_type,
        vid                          AS v_vid
      FROM log_depth_cal_vec
    )
    SELECT
      w.*,
      v.*,
      r.log_section_index            AS r_log_section_index,
      r.base_depth                   AS r_base_depth,
      r.base_depth_ouom              AS r_base_depth_ouom,
      r.bottom_left_x_pixel          AS r_bottom_left_x_pixel,
      r.bottom_left_y_pixel          AS r_bottom_left_y_pixel,
      r.bottom_right_x_pixel         AS r_bottom_right_x_pixel,
      r.bottom_right_y_pixel         AS r_bottom_right_y_pixel,
      r.create_date                  AS r_create_date,
      r.create_user_id               AS r_create_user_id,
      r.curve_bottom_left_x_pixel    AS r_curve_bottom_left_x_pixel,
      r.curve_bottom_left_y_pixel    AS r_curve_bottom_left_y_pixel,
      r.curve_bottom_right_x_pixel   AS r_curve_bottom_right_x_pixel,
      r.curve_bottom_right_y_pixel   AS r_curve_bottom_right_y_pixel, 
      r.curve_top_left_x_pixel       AS r_curve_top_left_x_pixel,
      r.curve_top_left_y_pixel       AS r_curve_top_left_y_pixel,
      r.curve_top_right_x_pixel      AS r_curve_top_right_x_pixel,
      r.curve_top_right_y_pixel      AS r_curve_top_right_y_pixel,
      r.header_bottom_left_x_pixel   AS r_header_bottom_left_x_pixel,
      r.header_bottom_left_y_pixel   AS r_header_bottom_left_y_pixel,
      r.header_bottom_right_x_pixel  AS r_header_bottom_right_x_pixel,
      r.header_bottom_right_y_pixel  AS r_header_bottom_right_y_pixel, 
      r.header_rotation              AS r_header_rotation,
      r.header_top_left_x_pixel      AS r_header_top_left_x_pixel,
      r.header_top_left_y_pixel      AS r_header_top_left_y_pixel,
      r.header_top_right_x_pixel     AS r_header_top_right_x_pixel,
      r.header_top_right_y_pixel     AS r_header_top_right_y_pixel,
      r.log_depth_cal_vid            AS r_log_depth_cal_vid,
      r.log_section_name             AS r_log_section_name,
      r.num_depth_cal_pts            AS r_num_depth_cal_pts,
      r.remark                       AS r_remark,
      r.source_registration_filename AS r_source_registration_filename,
      r.tif_file_identifier          AS r_tif_file_identifier,
      r.tif_file_path                AS r_tif_file_path,
      r.tif_filename                 AS r_tif_filename,
      r.top_depth                    AS r_top_depth,
      r.top_depth_ouom               AS r_top_depth_ouom,
      r.top_left_x_pixel             AS r_top_left_x_pixel,
      r.top_left_y_pixel             AS r_top_left_y_pixel,
      r.top_right_x_pixel            AS r_top_right_x_pixel,
      r.top_right_y_pixel            AS r_top_right_y_pixel,
      r.update_date                  AS r_update_date,
      r.update_user_id               AS r_update_user_id,
      r.well_id                      AS r_well_id
    FROM log_image_reg_log_section r
    JOIN w ON
      r.well_id = w.w_uwi
    JOIN v ON 
      r.log_depth_cal_vid = v.v_vid
    ) x`;

  const order = `ORDER BY w_uwi, r_log_section_index`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  return {
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
    case "decode_depth_registration":
      return (() => {
        try {
          const reg_points = [];
          const buf = Buffer.from(obj[key], "binary");
          for (let i = 12; i < buf.length; i += 28) {
            let depth = buf.slice(i, i + 8).readDoubleLE();
            let pixel = buf.slice(i + 12, i + 16).readInt32LE();
            reg_points.push({
              depth: depth,
              pixel: pixel,
            });
          }
          return reg_points;
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

  // LOG_IMAGE_REG_LOG_SECTION

  r_log_section_index: {
    ts_type: "number",
  },
  r_base_depth: {
    ts_type: "number",
  },
  r_base_depth_ouom: {
    ts_type: "string",
  },
  r_bottom_left_x_pixel: {
    ts_type: "number",
  },
  r_bottom_left_y_pixel: {
    ts_type: "number",
  },
  r_bottom_right_x_pixel: {
    ts_type: "number",
  },
  r_bottom_right_y_pixel: {
    ts_type: "number",
  },
  r_create_date: {
    ts_type: "date",
  },
  r_create_user_id: {
    ts_type: "string",
  },
  r_curve_bottom_left_x_pixel: {
    ts_type: "number",
  },
  r_curve_bottom_left_y_pixel: {
    ts_type: "number",
  },
  r_curve_bottom_right_x_pixel: {
    ts_type: "number",
  },
  r_curve_bottom_right_y_pixel: {
    ts_type: "number",
  },
  r_curve_top_left_x_pixel: {
    ts_type: "number",
  },
  r_curve_top_left_y_pixel: {
    ts_type: "number",
  },
  r_curve_top_right_x_pixel: {
    ts_type: "number",
  },
  r_curve_top_right_y_pixel: {
    ts_type: "number",
  },
  r_header_bottom_left_x_pixel: {
    ts_type: "number",
  },
  r_header_bottom_left_y_pixel: {
    ts_type: "number",
  },
  r_header_bottom_right_x_pixel: {
    ts_type: "number",
  },
  r_header_bottom_right_y_pixel: {
    ts_type: "number",
  },
  r_header_rotation: {
    ts_type: "number",
  },
  r_header_top_left_x_pixel: {
    ts_type: "number",
  },
  r_header_top_left_y_pixel: {
    ts_type: "number",
  },
  r_header_top_right_x_pixel: {
    ts_type: "number",
  },
  r_header_top_right_y_pixel: {
    ts_type: "number",
  },
  r_log_depth_cal_vid: {
    ts_type: "number",
  },
  r_log_section_name: {
    ts_type: "string",
  },
  r_num_depth_cal_pts: {
    ts_type: "number",
  },
  r_remark: {
    ts_type: "string",
  },
  r_source_registration_filename: {
    ts_type: "string",
  },
  r_tif_file_identifier: {
    ts_type: "string",
  },
  r_tif_file_path: {
    ts_type: "string",
  },
  r_tif_filename: {
    ts_type: "string",
  },
  r_top_depth: {
    ts_type: "number",
  },
  r_top_depth_ouom: {
    ts_type: "string",
  },
  r_top_left_x_pixel: {
    ts_type: "number",
  },
  r_top_left_y_pixel: {
    ts_type: "number",
  },
  r_top_right_x_pixel: {
    ts_type: "number",
  },
  r_top_right_y_pixel: {
    ts_type: "number",
  },
  r_update_date: {
    ts_type: "date",
  },
  r_update_user_id: {
    ts_type: "string",
  },
  r_well_id: {
    ts_type: "string",
  },

  // LOG_DEPTH_CAL_VEC

  v_bfile_data: {
    ts_type: "string",
  },
  v_blk_no: {
    ts_type: "number",
  },
  v_blob_data: {
    ts_type: "string",
    xform: "decode_depth_registration",
  },
  v_blob_data_orig: {
    ts_type: "string",
    xform: "blob_to_hex",
  },
  v_bytes_used: {
    ts_type: "number",
  },
  v_datatype: {
    ts_type: "number",
  },
  v_ow_rel_path: {
    ts_type: "string",
  },
  v_vec_storage_type: {
    ts_type: "string",
  },
  v_vid: {
    ts_type: "number",
  },
};

const prefixes = {
  w_: "well",
  v_: "log_depth_cal_vec",
  r_: "log_image_reg_log_section",
};

const asset_id_keys = ["w_uwi", "r_log_section_index"];

const well_id_keys = ["w_uwi"];

const default_chunk = 5000;

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
