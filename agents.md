# Agents Template

This repository uses a custom agent configuration to support Fabric pipeline scheduling.

## Agent files
- `.agent.md` — defines the custom agent behavior for weekday pipeline schedules.

## How to use
1. Create or identify the Fabric pipeline you want to schedule.
2. Ask the agent to create a weekday schedule, for example:
   - "Schedule my pipeline to run Monday through Friday."
   - "Set up a Fabric pipeline trigger on weekdays."
3. The agent will respond with the required schedule configuration and any next steps.

## Custom agent purpose
- Target domain: Microsoft Fabric pipeline orchestration
- Primary task: create and configure daily schedules for pipelines on weekdays
- Trigger conditions: requests that mention pipeline schedule, weekday trigger, or recurring daily runs

## Best practices
- If the pipeline name or workspace context is missing, the agent should ask for it.
- Use a cron expression or Fabric schedule format that covers Monday through Friday only.
- Default schedule time: 08:00 local time unless otherwise specified.

## Example schedule
- Cron expression: `0 8 * * 1-5`
- Description: Run every weekday at 08:00
