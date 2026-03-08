# CellarTracker MCP Server

An unofficial [CellarTracker](https://www.cellartracker.com/) [Remote MCP Server](https://modelcontextprotocol.io/) hosted on [Cloudflare Workers](https://workers.cloudflare.com/).

## Getting Started

[![Deploy to Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/michaelahern/cellartracker-mcp/tree/main)

This will deploy your MCP server to a URL like: `cellartracker-mcp.<your-account>.workers.dev/mcp`

### D1 Database Setup

This server stores wine inventory in a Cloudflare D1 database for efficient querying.

1. Create the D1 database:
   ```sh
   wrangler d1 create cellartracker-mcp
   ```
2. Copy the `database_id` from the output and paste it into `wrangler.jsonc` under `d1_databases`.
3. Deploy with `wrangler deploy`.
4. Use the `refresh_data` tool to populate the database with your CellarTracker inventory.

### Tools

- **search_wines** - Search your inventory with filters (producer, varietal, vintage, location, score). Returns up to 50 results.
- **get_cellar_stats** - Aggregate statistics: total bottles, value, top varietals/producers, wines in drinking window.
- **get_drinking_windows** - Wines in or approaching their drinking window. Returns up to 100 results.
- **get_wines_by_location** - Find wines by storage location. Returns up to 200 results.
- **refresh_data** - Fetch latest inventory from CellarTracker and store in D1. Run after making changes in CellarTracker.
