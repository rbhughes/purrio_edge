const purr_recent = "__purrRECENT__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

const select = `SELECT * FROM (
  WITH w AS (
    SELECT
      uwi                          AS w_uwi,
      row_changed_date             AS w_row_changed_date
    FROM well
    ${purr_recent}
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
  ),
  r AS (
    SELECT
      log_section_index            AS r_log_section_index,
      base_depth                   AS r_base_depth,
      base_depth_ouom              AS r_base_depth_ouom,
      bottom_left_x_pixel          AS r_bottom_left_x_pixel,
      bottom_left_y_pixel          AS r_bottom_left_y_pixel,
      bottom_right_x_pixel         AS r_bottom_right_x_pixel,
      bottom_right_y_pixel         AS r_bottom_right_y_pixel,
      create_date                  AS r_create_date,
      create_user_id               AS r_create_user_id,
      curve_bottom_left_x_pixel    AS r_curve_bottom_left_x_pixel,
      curve_bottom_left_y_pixel    AS r_curve_bottom_left_y_pixel,
      curve_bottom_right_x_pixel   AS r_curve_bottom_right_x_pixel,
      curve_bottom_right_y_pixel   AS r_curve_bottom_right_y_pixel, 
      curve_top_left_x_pixel       AS r_curve_top_left_x_pixel,
      curve_top_left_y_pixel       AS r_curve_top_left_y_pixel,
      curve_top_right_x_pixel      AS r_curve_top_right_x_pixel,
      curve_top_right_y_pixel      AS r_curve_top_right_y_pixel,
      header_bottom_left_x_pixel   AS r_header_bottom_left_x_pixel,
      header_bottom_left_y_pixel   AS r_header_bottom_left_y_pixel,
      header_bottom_right_x_pixel  AS r_header_bottom_right_x_pixel,
      header_bottom_right_y_pixel  AS r_header_bottom_right_y_pixel, 
      header_rotation              AS r_header_rotation,
      header_top_left_x_pixel      AS r_header_top_left_x_pixel,
      header_top_left_y_pixel      AS r_header_top_left_y_pixel,
      header_top_right_x_pixel     AS r_header_top_right_x_pixel,
      header_top_right_y_pixel     AS r_header_top_right_y_pixel,
      log_depth_cal_vid            AS r_log_depth_cal_vid,
      log_section_name             AS r_log_section_name,
      num_depth_cal_pts            AS r_num_depth_cal_pts,
      remark                       AS r_remark,
      source_registration_filename AS r_source_registration_filename,
      tif_file_identifier          AS r_tif_file_identifier,
      tif_file_path                AS r_tif_file_path,
      tif_filename                 AS r_tif_filename,
      top_depth                    AS r_top_depth,
      top_depth_ouom               AS r_top_depth_ouom,
      top_left_x_pixel             AS r_top_left_x_pixel,
      top_left_y_pixel             AS r_top_left_y_pixel,
      top_right_x_pixel            AS r_top_right_x_pixel,
      top_right_y_pixel            AS r_top_right_y_pixel,
      update_date                  AS r_update_date,
      update_user_id               AS r_update_user_id,
      well_id                      AS r_well_id
    FROM log_image_reg_log_section
  )
  SELECT
    w.*,
    v.*,
    r.*
  FROM w
  JOIN r ON
    r.r_well_id = w.w_uwi
  JOIN v ON 
    r.r_log_depth_cal_vid = v.v_vid
) x`;

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },
  w_row_changed_date: {
    ts_type: "date",
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

const asset_id_keys = ["w_uwi", "r_log_section_index"];

const default_chunk = 100; // 1000

const notes = [
  "Row changed dates are not implemented in LOG_IMAGE_REG_LOG_SECTION; using WELL instead.",
  "TODO: get chgdate from LIC files? (would be extremely slow)",
]

const order = `ORDER BY w_uwi, r_log_section_index`;

const prefixes = {
  w_: "well",
  v_: "log_depth_cal_vec",
  r_: "log_image_reg_log_section",
};

const well_id_keys = ["w_uwi"];


///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = () => {
  return {
    asset_id_keys,
    default_chunk,
    notes,
    order,
    purr_delimiter,
    purr_null,
    purr_recent,
    prefixes,
    select,
    well_id_keys,
    xforms
  };
};
