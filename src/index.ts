import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { McpAgent } from 'agents/mcp';

export class CellarTrackerMCP extends McpAgent {
    server = new McpServer({
        name: 'cellartracker',
        title: 'CellarTracker',
        version: '1.0.0',
        description: 'An MCP server that integrates with CellarTracker to provide access to your wine inventory and related data.',
        websiteUrl: 'https://github.com/michaelahern/cellartracker-mcp/',
        icons: [
            { src: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAJgAAACYCAMAAAAvHNATAAABKVBMVEVSJSRTJiVVKSheNDNgNzViOjhqRUJwTElwTUpyTkt0UU50Uk91Uk96WVZ/YFyAYF2AYV2BYl6CY1+DZWGJbWiKbmmMcWyNcW2Rd3KSeHOUJSuUJiyWKS+WfXiaNDmagn2bNzubhH+dOj6dh4GeiIKgioShRUikTE+kkIqlTU+lTlGmUVOnUlSnUlWnk42olY+qWVusYGGtYGGtYWKtYmOuY2SvZWWvnpexoJmybW2ybm2yopu0cXC0cXG2d3a3eHe5fXu6raW7goC8hIK9h4S9iIa+iojBkI3Bta7Ck5DDlZLGnprHoJzIop3Lw7rNrafOxr7Qta/RysHWw7vW0MfXxr/X0cjZysLa1szb0Mfc0cne1s3f3NLg3NLg3dPh3dPh39Xi39Xi4NaMLwA0AAAB6UlEQVR4XuzOMQEAAAwCoGW1f4jF0AMScBklJibWISYmJiYmJiYmJiYmJiYmJiYm9uzYbQqCQBCH8Xv8jVgCisrAKoMiK7Y3eiMiJDIk9v6XSOirALEzzrL4O8GDoM7M63baa63Xq0IyL0zjEsPoZ9ArE0aj2eacEoaZaxd01PJJFJbHoNXYGpKwBcglFGGPAPQuBGE7MGjl9mFjcDjah3XAQb2twxRYTD62YQF4tA+pXRiq0r87GgZk/4QZVCd09Yk1XQ1DHVaHcfPlray/Y5kH/0qB6UJ+HvNigpWf+eW3JH/3SvlNXP52IX/tcfM+9tXoUOeowwYBGHXYqMNGHTbqsFGHBZgrivDzcnNiAzy8EMAnhAUIiwKBuCQQyMrLy6vqO4ZQ0WFBEgB27BgHICCIwvDDQmhQcAAKlVriAipRiUJk738KZ5CZkVfsf4KvsWbGQa9i9TowP8TQrb1VYD3U6zRgC/SLdjnsTmHQKIfNsKgWwy4Hi3Ip7GlgkpPBjimDTdE32Fbirz7BToATVrHCElYYrAuwACP+KsM75llfftp/Je10QTuP0U6wtDM/7ZZEu1eybuK0twvWaw/nfextxw4JAAAAAAT9f211BC4ga3U+MDAwMDAwMDAwMDAwMDAwMDAwMDCwAKupr8IUOwjHAAAAAElFTkSuQmCC' }
        ]
    });

    async init() {
        this.ct_tool('wine', 'Wine List', 'Get your current wine library list from CellarTracker.', 'List');
        this.ct_tool('wine_bottles', 'Wine Bottles', 'Get your current wine bottle inventory from CellarTracker.', 'Inventory');
        this.ct_tool('wine_consumed', 'Wine Consumed', 'Get your consumed wine bottles from CellarTracker.', 'Consumed');
        this.ct_tool('wine_pending', 'Wine Pending Delivery', 'Get your wine inventory pending delivery from CellarTracker.', 'Pending');
        this.ct_tool('wine_purchases', 'Wine Purchases', 'Get your wine purchase history from CellarTracker.', 'Purchase');
    }

    async ct_tool(name: string, title: string, description: string, table: 'List' | 'Inventory' | 'Purchase' | 'Pending' | 'Consumed' | 'Availability') {
        this.server.registerTool(name, {
            title: title,
            description: description
        },
        async () => {
            const username = await this.env.CELLARTRACKER_USERNAME.get();
            const password = await this.env.CELLARTRACKER_PASSWORD.get();
            const url = `https://www.cellartracker.com/xlquery.asp?User=${encodeURIComponent(username)}&Password=${encodeURIComponent(password)}&Format=tab&Table=${encodeURIComponent(table)}`;
            const response = await fetch(url);

            if (!response.ok) {
                return {
                    content: [{ type: 'text', text: `Failed to fetch data from CellarTracker: ${response.status} ${response.statusText}` }],
                    isError: true
                };
            }

            let text = 'The data below is in tab-delimited format. Some notes on the dataset:\n';
            text += '- The first line contains column headers.\n';
            text += '- The column header RR should be JD to represent professional wine ratings from Jeb Dunnuck.\n';
            text += '- Each subsequent line represents a record in your CellarTracker data.\n';
            text += '- The columns included depend on the table you are querying (e.g., List, Inventory, etc.).\n';
            text += '- You can parse this data into a structured format (like JSON) for easier analysis.\n\n';
            text += await response.text();

            return {
                content: [{ type: 'text', text }]
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
