const ASSETS = [
  'completion',
  'core',
  'dst',
  'formation',
  'ip',
  'perforation',
  'production',
  'raster_log',
  'survey',
  'vector_log',
  'well'
]

export const assetStatsSQL = () => {
  const cmds: string[] = []

  ASSETS.map((asset) => {
    const sql = `select 
    '${asset}' as asset,
    count(*) as total_records,
    round(avg(EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, updated_at))
      / 86400.0), 2) AS avg_elapsed_days,
    round(stddev( EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, updated_at))
      / 86400.0 ), 4) AS stddev_elapsed_days,
    round(min( EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, updated_at))
      / 86400.0 ), 2) AS min_elapsed_days,
    round(max( EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, updated_at))
      / 86400.0 ), 2) AS max_elapsed_days,
    round(sum((case when EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, updated_at))
      / 86400.0 <= 91 then 1 else 0 end)) / count(*) *100, 2)
      AS pct_updated_last_3_month,
    round(sum((case when EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, updated_at))
      / 86400.0 <= 182 then 1 else 0 end)) / count(*) *100, 2)
      AS pct_updated_last_6_month,
    round(sum((case when EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, updated_at))
      / 86400.0 <= 273 then 1 else 0 end)) / count(*) *100, 2)
      AS pct_updated_last_9_month,
    round(sum((case when EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, updated_at))
      / 86400.0 <= 365 then 1 else 0 end)) / count(*) *100, 2)
      AS pct_updated_last_12_month
    from ${asset}`

    cmds.push(sql)
  })

  return cmds.join('\nunion\n') + '\norder by asset'
}
