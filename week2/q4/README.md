# NOTES
Deployment code:

```bash
prefect deployment build ./week2/q4/etl_web_to_gcs_gh.py:etl_parent_flow_gh \
-n "Github Storage Flow" \
-sb github/gh-dtc-storage/week2/q4 \
-o ./week2/q4/etl_web_to_gcs_gh-deployment.yaml \
--apply
```

`-n`: deployment name \
`-sb`: storage block, refers to the created github block. Also, appending subfolders is possible \
`-o` : output, location and filename \
`--apply` : saves and updates deployment in prefect orion/cloud