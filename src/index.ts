import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { McpAgent } from 'agents/mcp';
import { z } from 'zod';
import { initSchema, truncateAndInsertBottles, truncateAndInsertWines, searchWines, getCellarStats, getDrinkingWindows, getBottlesByLocation } from './db.js';
import { fetchBottles, fetchWines } from './fetcher.js';

function formatResults(results: unknown[], label: string): { content: { type: 'text'; text: string }[] } {
    if (results.length === 0) {
        return {
            content: [{
                type: 'text' as const,
                text: `No ${label} found. You may need to run the refresh_data tool first to populate the database.`
            }]
        };
    }
    return {
        content: [{ type: 'text' as const, text: JSON.stringify(results, null, 2) }]
    };
}

export class CellarTrackerMCP extends McpAgent {
    server = new McpServer({
        name: 'cellartracker',
        title: 'CellarTracker',
        version: '1.0.0',
        description: 'An MCP server that integrates with CellarTracker to provide access to your wine inventory and related data.',
        websiteUrl: 'https://github.com/michaelahern/cellartracker-mcp/',
        icons: [
            { src: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAJgAAACYCAMAAAAvHNATAAABKVBMVEVSJSRTJiVVKSheNDNgNzViOjhqRUJwTElwTUpyTkt0UU50Uk91Uk96WVZ/YFyAYF2AYV2BYl6CY1+DZWGJbWiKbmmMcWyNcW2Rd3KSeHOUJSuUJiyWKS+WfXiaNDmagn2bNzubhH+dOj6dh4GeiIKgioShRUikTE+kkIqlTU+lTlGmUVOnUlSnUlWnk42olY+qWVusYGGtYGGtYWKtYmOuY2SvZWWvnpexoJmybW2ybm2yopu0cXC0cXG2d3a3eHe5fXu6raW7goC8hIK9h4S9iIa+iojBkI3Bta7Ck5DDlZLGnprHoJzIop3Lw7rNrafOxr7Qta/RysHWw7vW0MfXxr/X0cjZysLa1szb0Mfc0cne1s3f3NLg3NLg3dPh3dPh39Xi39Xi4NaMLwA0AAAB6UlEQVR4XuzOMQEAAAwCoGW1f4jF0AMScBklJibWISYmJiYmJiYmJiYmJiYmJiYm9uzYbQqCQBCH8Xv8jVgCisrAKoMiK7Y3eiMiJDIk9v6XSOirALEzzrL4O8GDoM7M63baa63Xq0IyL0zjEsPoZ9ArE0aj2eacEoaZaxd01PJJFJbHoNXYGpKwBcglFGGPAPQuBGE7MGjl9mFjcDjah3XAQb2twxRYTD62YQF4tA+pXRiq0r87GgZk/4QZVCd09Yk1XQ1DHVaHcfPlray/Y5kH/0qB6UJ+HvNigpWf+eW3JH/3SvlNXP52IX/tcfM+9tXoUOeowwYBGHXYqMNGHTbqsFGHBZgrivDzcnNiAzy8EMAnhAUIiwKBuCQQyMrLy6vqO4ZQ0WFBEgB27BgHICCIwvDDQmhQcAAKlVriAipRiUJk738KZ5CZkVfsf4KvsWbGQa9i9TowP8TQrb1VYD3U6zRgC/SLdjnsTmHQKIfNsKgWwy4Hi3Ip7GlgkpPBjimDTdE32Fbirz7BToATVrHCElYYrAuwACP+KsM75llfftp/Je10QTuP0U6wtDM/7ZZEu1eybuK0twvWaw/nfextxw4JAAAAAAT9f211BC4ga3U+MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDCwAKupr8IUOwjHAAAAAElFTkSuQmCC' }
        ]
    });

    async init() {
        this.server.registerTool('search_wines', {
            title: 'Search Wines',
            description: 'Search your wine inventory with optional filters. Returns up to 50 matching wines.',
            inputSchema: {
                producer: z.string().optional().describe('Filter by producer name (partial match)'),
                varietal: z.string().optional().describe('Filter by varietal/grape (partial match)'),
                vintage_min: z.number().optional().describe('Minimum vintage year'),
                vintage_max: z.number().optional().describe('Maximum vintage year'),
                location: z.string().optional().describe('Filter by storage location (partial match)'),
                min_score: z.number().optional().describe('Minimum score from any critic (CT, WA, WS, VM, JD, MY)'),
                in_stock_only: z.boolean().optional().describe('Only show wines in stock (default: true)')
            }
        }, async (params) => {
            const db = this.env.CELLARTRACKER_DB;
            const result = await searchWines(db, params);
            return formatResults(result.results, 'wines');
        });

        this.server.registerTool('get_cellar_stats', {
            title: 'Cellar Statistics',
            description: 'Get aggregate statistics about your cellar: total bottles, total value, top varietals, top producers, and wines in drinking window.'
        }, async () => {
            const db = this.env.CELLARTRACKER_DB;
            const stats = await getCellarStats(db);
            return {
                content: [{ type: 'text' as const, text: JSON.stringify(stats, null, 2) }]
            };
        });

        this.server.registerTool('get_drinking_windows', {
            title: 'Drinking Windows',
            description: 'Find wines that are currently in or approaching their drinking window. Returns up to 100 wines ordered by end of drinking window (most urgent first).',
            inputSchema: {
                within_years: z.number().optional().describe('Number of years from now to look ahead (default: 3)')
            }
        }, async (params) => {
            const db = this.env.CELLARTRACKER_DB;
            const withinYears = params.within_years ?? 3;
            const result = await getDrinkingWindows(db, withinYears);
            return formatResults(result.results, 'wines in drinking window');
        });

        this.server.registerTool('get_bottles_by_location', {
            title: 'Bottles by Location',
            description: 'Find individual bottles stored in a specific location, including bin placement. Returns up to 200 bottles.',
            inputSchema: {
                location: z.string().describe('Storage location to search for (partial match)')
            }
        }, async (params) => {
            const db = this.env.CELLARTRACKER_DB;
            const result = await getBottlesByLocation(db, params.location);
            return formatResults(result.results, 'bottles at that location');
        });

        this.server.registerTool('refresh_data', {
            title: 'Refresh Data',
            description: 'Fetch the latest inventory data from CellarTracker and store it in the database. Run this after making changes in CellarTracker or when first setting up.'
        }, async () => {
            const db = this.env.CELLARTRACKER_DB;
            const username = await this.env.CELLARTRACKER_USERNAME.get();
            const password = await this.env.CELLARTRACKER_PASSWORD.get();

            await initSchema(db);
            const [bottleResult, wineResult] = await Promise.all([
                fetchBottles(username, password),
                fetchWines(username, password)
            ]);
            await truncateAndInsertBottles(db, bottleResult.rows);
            await truncateAndInsertWines(db, wineResult.rows);

            const counts = await db.batch([
                db.prepare('SELECT COUNT(*) AS count FROM bottles'),
                db.prepare('SELECT COUNT(*) AS count FROM wines')
            ]);
            const bottleDbCount = (counts[0]?.results[0] as Record<string, unknown> | undefined)?.['count'] ?? '?';
            const wineDbCount = (counts[1]?.results[0] as Record<string, unknown> | undefined)?.['count'] ?? '?';

            const wd = wineResult.diagnostics;
            const bd = bottleResult.diagnostics;
            const lines = [
                `Refreshed inventory data at ${new Date().toISOString()}.`,
                `Wines: ${wd.responseBytes} bytes fetched, ${wd.parsedRows} rows parsed, ${wineDbCount} stored in DB.`,
                `Bottles: ${bd.responseBytes} bytes fetched, ${bd.parsedRows} rows parsed, ${bottleDbCount} stored in DB.`
            ];
            if (wd.parseErrors > 0) {
                lines.push(`Wine parse errors: ${wd.parseErrors}. First: ${wd.firstError ?? 'unknown'}`);
            }
            if (bd.parseErrors > 0) {
                lines.push(`Bottle parse errors: ${bd.parseErrors}. First: ${bd.firstError ?? 'unknown'}`);
            }

            return {
                content: [{ type: 'text' as const, text: lines.join('\n') }]
            };
        });
    }
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext) {
        const authToken = await env.CELLARTRACKER_MCP_AUTH_TOKEN.get();
        const authHeader = request.headers.get('Authorization');
        if (!authHeader || authHeader !== `Bearer ${authToken}`) {
            return new Response('Unauthorized', { status: 401 });
        }

        const url = new URL(request.url);

        if (url.pathname === '/mcp') {
            return CellarTrackerMCP.serve('/mcp').fetch(request, env, ctx);
        }

        return new Response('Not found', { status: 404 });
    }
};
