import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { McpAgent } from 'agents/mcp';
import { z } from 'zod';

import { getCellarStats, initSchema, searchBottles, searchWines, truncateAndInsertBottles, truncateAndInsertReviews, truncateAndInsertWines } from './db.js';
import { fetchBottles, fetchReviews, fetchWines } from './fetcher.js';

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

        this.server.registerTool('search_bottles', {
            title: 'Search Bottles',
            description: 'Search individual bottles in your cellar with optional filters. Returns up to 200 bottles with location/bin details.',
            inputSchema: {
                bottle_state: z.enum(['in_cellar', 'consumed', 'pending_delivery']).optional().describe('Filter by bottle state (default: in_cellar)'),
                vintage_min: z.number().optional().describe('Minimum vintage year'),
                vintage_max: z.number().optional().describe('Maximum vintage year'),
                location: z.string().optional().describe('Filter by storage location (partial match)'),
                country: z.string().optional().describe('Filter by country (partial match)'),
                region: z.string().optional().describe('Filter by region (partial match)'),
                sub_region: z.string().optional().describe('Filter by sub-region (partial match)'),
                appellation: z.string().optional().describe('Filter by appellation (partial match)'),
                producer: z.string().optional().describe('Filter by producer name (partial match)'),
                type: z.string().optional().describe('Filter by wine type, e.g. Red, White, Sparkling (partial match)'),
                varietal: z.string().optional().describe('Filter by varietal/grape (partial match)'),
                min_score: z.number().optional().describe('Minimum score from any critic (JD, TWP, VM, WA)'),
                in_drinking_window: z.boolean().optional().describe('Filter by whether the bottle is currently in its drinking window')
            }
        }, async (params) => {
            const db = this.env.CELLARTRACKER_DB;
            const result = await searchBottles(db, params);
            return formatResults(result.results, 'bottles');
        });

        this.server.registerTool('search_wines', {
            title: 'Search Wines',
            description: 'Search wines in your cellar/collection with optional filters. Returns up to 100 matching wines. Use get_cellar_stats to get example values for varietal, producer, region, and other attributes. Use search_bottles tool to see individual bottles of a wine with the bottles\' location, purchase, and consumption data.',
            inputSchema: {
                vintage_min: z.number().optional().describe('Minimum vintage year'),
                vintage_max: z.number().optional().describe('Maximum vintage year'),
                type: z.string().optional().describe('Filter by wine type, e.g. Red, White, Sparkling (partial match)'),
                varietal: z.string().optional().describe('Filter by varietal/grape (partial match)'),
                producer: z.string().optional().describe('Filter by producer name (partial match)'),
                country: z.string().optional().describe('Filter by country (partial match)'),
                region: z.string().optional().describe('Filter by region (partial match)'),
                sub_region: z.string().optional().describe('Filter by sub-region (partial match)'),
                appellation: z.string().optional().describe('Filter by appellation (partial match)'),
                designation: z.string().optional().describe('Filter by designation (partial match)'),
                vineyard: z.string().optional().describe('Filter by vineyard (partial match)'),
                min_score: z.number().optional().describe('Filter by minimum score from any critic (JD: Jeb Dunnuck, TWP: The Wine Palate, VM: Vinous, WA: Wine Advocate)'),
                in_cellar_only: z.boolean().optional().describe('Only show wines in stock in your cellar today and not wines pending delivery'),
                in_drinking_window: z.boolean().optional().describe('Only show wines currently in their drinking window now')
            }
        }, async (params) => {
            const db = this.env.CELLARTRACKER_DB;
            const result = await searchWines(db, params);

            if (result.results.length === 0) {
                return {
                    content: [{ type: 'text' as const, text: 'No wines found matching your criteria.' }]
                };
            }
            return {
                content: [{ type: 'text' as const, text: JSON.stringify(result.results, null, 2) }]
            };
        });

        this.server.registerTool('refresh_data', {
            title: 'Refresh Data',
            description: 'Fetch the latest wine cellar inventory data from CellarTracker and store it in the database. Run this after making changes in CellarTracker or when first setting up.'
        }, async () => {
            const db = this.env.CELLARTRACKER_DB;
            const username = await this.env.CELLARTRACKER_USERNAME.get();
            const password = await this.env.CELLARTRACKER_PASSWORD.get();

            await initSchema(db);

            const [wineResult, bottleResult, reviewResult] = await Promise.all([
                fetchWines(username, password),
                fetchBottles(username, password),
                fetchReviews(username, password)
            ]);

            const insertErrors = [
                await truncateAndInsertWines(db, wineResult.rows),
                await truncateAndInsertBottles(db, bottleResult.rows),
                await truncateAndInsertReviews(db, reviewResult.rows)
            ].filter((e): e is string => e !== null);

            const counts = await db.batch([
                db.prepare('SELECT COUNT(*) AS count FROM wines'),
                db.prepare('SELECT COUNT(*) AS count FROM bottles'),
                db.prepare('SELECT COUNT(*) AS count FROM reviews')
            ]);

            const wineDbCount = (counts[0]?.results[0] as Record<string, unknown> | undefined)?.['count'] ?? '?';
            const bottleDbCount = (counts[1]?.results[0] as Record<string, unknown> | undefined)?.['count'] ?? '?';
            const reviewDbCount = (counts[2]?.results[0] as Record<string, unknown> | undefined)?.['count'] ?? '?';

            const lines = [
                `Refreshed cellar inventory data at ${new Date().toISOString()}.`,
                `Wines: ${wineResult.diagnostics.responseBytes} bytes fetched, ${wineResult.diagnostics.parsedRows} rows parsed, ${wineDbCount} stored in DB.`,
                `Bottles: ${bottleResult.diagnostics.responseBytes} bytes fetched, ${bottleResult.diagnostics.parsedRows} rows parsed, ${bottleDbCount} stored in DB.`,
                `Reviews: ${reviewResult.diagnostics.responseBytes} bytes fetched, ${reviewResult.diagnostics.parsedRows} rows parsed, ${reviewDbCount} stored in DB.`
            ];

            if (wineResult.diagnostics.parseErrors > 0) {
                lines.push(`Wine parse errors: ${wineResult.diagnostics.parseErrors}. First: ${wineResult.diagnostics.firstError ?? 'unknown'}`);
            }
            if (bottleResult.diagnostics.parseErrors > 0) {
                lines.push(`Bottle parse errors: ${bottleResult.diagnostics.parseErrors}. First: ${bottleResult.diagnostics.firstError ?? 'unknown'}`);
            }
            if (reviewResult.diagnostics.parseErrors > 0) {
                lines.push(`Review parse errors: ${reviewResult.diagnostics.parseErrors}. First: ${reviewResult.diagnostics.firstError ?? 'unknown'}`);
            }
            for (const err of insertErrors) {
                lines.push(`DB insert error: ${err}`);
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
