# Neural Search QA

## Model Deployment Guidelines

### Important Notes for Model Handling

1. **Non-ML Node Deployment**
   - To deploy models on non-ML nodes, set `plugins.ml_commons.only_run_on_ml_node` to `false`
   - **Warning**: Resetting this setting will undeploy all currently deployed models

2. **Avoid Duplicate Deployments**
   - Do not attempt to deploy already deployed models
   - This can cause tasks to get stuck in `RUNNING` state (possible ml-commons bug)

3. **Node Replacement Timing**
   - Avoid deploying models immediately after node replacement
   - This can interfere with the auto-deploy feature (possible ml-commons bug)
