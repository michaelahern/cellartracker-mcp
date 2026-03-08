export async function initSchema(db: D1Database) {
    await db.batch([
        db.prepare(
            'CREATE TABLE IF NOT EXISTS bottles (Barcode INTEGER PRIMARY KEY, iWine INTEGER, Location TEXT, Bin TEXT, StoreName TEXT, PurchaseDate TEXT, Size TEXT, Vintage INTEGER, Wine TEXT, Country TEXT, Region TEXT, SubRegion TEXT, Appellation TEXT, Producer TEXT, Type TEXT, Color TEXT, Category TEXT, Varietal TEXT, Designation TEXT, Vineyard TEXT, WA INTEGER, VM INTEGER, JD INTEGER, CT INTEGER, MY INTEGER, BeginConsume INTEGER, EndConsume INTEGER)'
        ),
        db.prepare(
            'CREATE TABLE IF NOT EXISTS wines (iWine INTEGER PRIMARY KEY, Quantity INTEGER, Pending INTEGER, Size TEXT, Vintage INTEGER, Wine TEXT, Country TEXT, Region TEXT, SubRegion TEXT, Appellation TEXT, Producer TEXT, Type TEXT, Color TEXT, Category TEXT, Varietal TEXT, Designation TEXT, Vineyard TEXT, WA INTEGER, VM INTEGER, JD INTEGER, CT INTEGER, MY INTEGER, BeginConsume INTEGER, EndConsume INTEGER)'
        ),
        db.prepare(
            'CREATE TABLE IF NOT EXISTS reviews (iReview TEXT PRIMARY KEY, iWine TEXT, Publication TEXT, ReviewDate TEXT, Reviewer TEXT, Score TEXT, ReviewText TEXT, ReviewURL TEXT, BeginConsume TEXT, EndConsume TEXT)'
        )
    ]);
}

const BOTTLE_COLUMNS = [
    'Barcode', 'iWine', 'Location', 'Bin', 'StoreName', 'PurchaseDate',
    'Size', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Color', 'Category', 'Varietal', 'Designation', 'Vineyard',
    'WA', 'VM', 'JD', 'CT', 'MY', 'BeginConsume', 'EndConsume'
];

const WINE_COLUMNS = [
    'iWine', 'Quantity', 'Pending',
    'Size', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Color', 'Category', 'Varietal', 'Designation', 'Vineyard',
    'WA', 'VM', 'JD', 'CT', 'MY', 'BeginConsume', 'EndConsume'
];

const REVIEW_COLUMNS = [
    'iReview', 'iWine', 'Publication', 'ReviewDate', 'Reviewer',
    'Score', 'ReviewText', 'ReviewURL', 'BeginConsume', 'EndConsume'
];

async function truncateAndInsert(db: D1Database, table: string, columns: string[], rows: Record<string, unknown>[]) {
    const quotedColumns = columns.map(c => `"${c}"`).join(', ');
    const placeholders = columns.map(() => '?').join(', ');
    const insertSQL = `INSERT OR REPLACE INTO ${table} (${quotedColumns}) VALUES (${placeholders})`;

    await db.prepare(`DELETE FROM ${table}`).run();

    const statements: D1PreparedStatement[] = [];
    for (const row of rows) {
        const values = columns.map((col) => {
            const val = row[col];
            return val === '' || val === undefined ? null : val;
        });
        statements.push(db.prepare(insertSQL).bind(...values));
    }

    const BATCH_SIZE = 20;
    for (let i = 0; i < statements.length; i += BATCH_SIZE) {
        await db.batch(statements.slice(i, i + BATCH_SIZE));
    }
}

export async function truncateAndInsertBottles(db: D1Database, rows: Record<string, unknown>[]) {
    await truncateAndInsert(db, 'bottles', BOTTLE_COLUMNS, rows);
}

export async function truncateAndInsertWines(db: D1Database, rows: Record<string, unknown>[]) {
    await truncateAndInsert(db, 'wines', WINE_COLUMNS, rows);
}

export async function truncateAndInsertReviews(db: D1Database, rows: Record<string, unknown>[]) {
    await truncateAndInsert(db, 'reviews', REVIEW_COLUMNS, rows);
}

export async function getCellarStats(db: D1Database) {
    const results = await db.batch<Record<string, unknown>>([
        db.prepare(`
            SELECT
                COALESCE(SUM(Quantity), 0) AS bottles_in_cellar,
                COALESCE(SUM(Pending), 0) AS bottles_pending_delivery,
                COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total,
                COUNT(*) AS unique_wines
            FROM wines
        `),
        db.prepare(`
            SELECT Location AS location, COUNT(*) AS bottles_total
            FROM bottles
            WHERE Location IS NOT NULL AND Location != ''
            GROUP BY Location
            ORDER BY bottles_total DESC
            LIMIT 100
        `),
        db.prepare(`
            SELECT
                CASE
                    WHEN EndConsume < CAST(strftime('%Y', 'now') AS INTEGER) THEN 'past'
                    WHEN BeginConsume <= CAST(strftime('%Y', 'now') AS INTEGER) AND EndConsume >= CAST(strftime('%Y', 'now') AS INTEGER) THEN 'now'
                    WHEN BeginConsume > CAST(strftime('%Y', 'now') AS INTEGER) THEN 'starting ' || CAST(BeginConsume AS TEXT)
                    ELSE 'unknown'
                END AS window,
                COALESCE(SUM(Quantity), 0) AS bottles_in_cellar,
                COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE BeginConsume IS NOT NULL AND EndConsume IS NOT NULL
            GROUP BY window
            ORDER BY window ASC
        `),
        db.prepare(`
            SELECT Type AS type, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE Type IS NOT NULL AND Type != '' AND Type != 'Unknown'
            GROUP BY Type
            ORDER BY bottles_total DESC
            LIMIT 20
        `),
        db.prepare(`
            SELECT Varietal AS varietal, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total,
                ROUND(AVG(CASE WHEN VM IS NOT NULL THEN VM END), 1) AS avg_score_vm, ROUND(AVG(CASE WHEN JD IS NOT NULL THEN JD END), 1) AS avg_score_jd,
                ROUND(AVG(CASE WHEN WA IS NOT NULL THEN WA END), 1) AS avg_score_wa, ROUND(AVG(CASE WHEN CT IS NOT NULL THEN CT END), 1) AS avg_score_ct
            FROM wines
            WHERE Varietal IS NOT NULL AND Varietal != '' AND Varietal != 'Unknown'
            GROUP BY Varietal
            ORDER BY bottles_total DESC
            LIMIT 20
        `),
        db.prepare(`
            SELECT Producer AS producer, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total,
                ROUND(AVG(CASE WHEN VM IS NOT NULL THEN VM END), 1) AS avg_score_vm, ROUND(AVG(CASE WHEN JD IS NOT NULL THEN JD END), 1) AS avg_score_jd,
                ROUND(AVG(CASE WHEN WA IS NOT NULL THEN WA END), 1) AS avg_score_wa, ROUND(AVG(CASE WHEN CT IS NOT NULL THEN CT END), 1) AS avg_score_ct
            FROM wines
            WHERE Producer IS NOT NULL AND Producer != '' AND Producer != 'Unknown'
            GROUP BY Producer
            ORDER BY bottles_total DESC
            LIMIT 20
        `),
        db.prepare(`
            SELECT Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE Country IS NOT NULL AND Country != '' AND Country != 'Unknown'
            GROUP BY Country
            ORDER BY bottles_total DESC
            LIMIT 10
        `),
        db.prepare(`
            SELECT Region AS region, Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE Region IS NOT NULL AND Region != '' AND Region != 'Unknown'
            GROUP BY Region
            ORDER BY bottles_total DESC
            LIMIT 10
        `),
        db.prepare(`
            SELECT SubRegion AS sub_region, Region AS region, Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE SubRegion IS NOT NULL AND SubRegion != '' AND SubRegion != 'Unknown'
            GROUP BY SubRegion
            ORDER BY bottles_total DESC
            LIMIT 10
        `),
        db.prepare(`
            SELECT Appellation AS appellation, SubRegion AS sub_region, Region AS region, Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE Appellation IS NOT NULL AND Appellation != '' AND Appellation != 'Unknown'
            GROUP BY Appellation
            ORDER BY bottles_total DESC
            LIMIT 10
        `)
    ]);

    const totals = results[0] ?? { results: [] };
    const locations = results[1] ?? { results: [] };
    const drinkingWindows = results[2] ?? { results: [] };
    const types = results[3] ?? { results: [] };
    const varietals = results[4] ?? { results: [] };
    const producers = results[5] ?? { results: [] };
    const countries = results[6] ?? { results: [] };
    const regions = results[7] ?? { results: [] };
    const subRegions = results[8] ?? { results: [] };
    const appellations = results[9] ?? { results: [] };

    return {
        totals: totals.results[0],
        locations: locations.results,
        drinking_window: drinkingWindows.results,
        top_types: types.results,
        top_varietals: varietals.results,
        top_producers: producers.results,
        top_countries: countries.results,
        top_regions: regions.results,
        top_sub_regions: subRegions.results,
        top_appellations: appellations.results
    };
}

export interface BottleSearchFilters {
    vintage_min?: number | undefined;
    vintage_max?: number | undefined;
    location?: string | undefined;
    country?: string | undefined;
    region?: string | undefined;
    sub_region?: string | undefined;
    appellation?: string | undefined;
    producer?: string | undefined;
    type?: string | undefined;
    varietal?: string | undefined;
    min_score?: number | undefined;
    in_drinking_window?: boolean | undefined;
}

export async function searchBottles(db: D1Database, filters: BottleSearchFilters) {
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (filters.vintage_min !== undefined) {
        conditions.push('b.Vintage >= ?');
        params.push(filters.vintage_min);
    }
    if (filters.vintage_max !== undefined) {
        conditions.push('b.Vintage <= ?');
        params.push(filters.vintage_max);
    }
    if (filters.location) {
        conditions.push('b.Location LIKE ?');
        params.push(`%${filters.location}%`);
    }
    if (filters.country) {
        conditions.push('b.Country LIKE ?');
        params.push(`%${filters.country}%`);
    }
    if (filters.region) {
        conditions.push('b.Region LIKE ?');
        params.push(`%${filters.region}%`);
    }
    if (filters.sub_region) {
        conditions.push('b.SubRegion LIKE ?');
        params.push(`%${filters.sub_region}%`);
    }
    if (filters.appellation) {
        conditions.push('b.Appellation LIKE ?');
        params.push(`%${filters.appellation}%`);
    }
    if (filters.producer) {
        conditions.push('b.Producer LIKE ?');
        params.push(`%${filters.producer}%`);
    }
    if (filters.type) {
        conditions.push('b.Type LIKE ?');
        params.push(`%${filters.type}%`);
    }
    if (filters.varietal) {
        conditions.push('b.Varietal LIKE ?');
        params.push(`%${filters.varietal}%`);
    }
    if (filters.min_score !== undefined) {
        conditions.push('(b.CT >= ? OR b.WA >= ? OR b.VM >= ? OR b.JD >= ? OR b.MY >= ?)');
        params.push(filters.min_score, filters.min_score, filters.min_score, filters.min_score, filters.min_score);
    }
    if (filters.in_drinking_window === true) {
        conditions.push('b.BeginConsume IS NOT NULL AND b.EndConsume IS NOT NULL AND b.BeginConsume <= CAST(strftime(\'%Y\', \'now\') AS INTEGER) AND b.EndConsume >= CAST(strftime(\'%Y\', \'now\') AS INTEGER)');
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql = `
        SELECT b.Wine AS wine, b.Vintage AS vintage, b.Size AS size,
            b.Location AS location, (SELECT GROUP_CONCAT(bin_summary, '; ') FROM (SELECT b2.Bin || ' (x' || COUNT(*) || ')' AS bin_summary FROM bottles b2 WHERE b2.iWine = b.iWine AND b2.Location = b.Location GROUP BY b2.Bin)) AS bins,
            COUNT(*) AS bottles_at_location, COALESCE(w.Quantity, 0) AS bottles_in_cellar, COALESCE(w.Quantity, 0) + COALESCE(w.Pending, 0) AS bottles_total,
            b.Country AS country, b.Region AS region, b.SubRegion AS sub_region, b.Appellation AS appellation,
            b.Producer AS producer, b.Type AS type, b.Varietal AS varietal, b.Designation AS designation, b.Vineyard AS vineyard,
            b.VM AS score_vm, b.JD AS score_jd, b.WA AS score_wa, b.CT AS score_ct, b.MY AS score_my,
            b.BeginConsume AS begin_consume_year, b.EndConsume AS end_consume_year,
            CASE
                WHEN b.BeginConsume IS NULL OR b.EndConsume IS NULL THEN NULL
                WHEN b.EndConsume < CAST(strftime('%Y', 'now') AS INTEGER) THEN 'past'
                WHEN b.BeginConsume <= CAST(strftime('%Y', 'now') AS INTEGER) AND b.EndConsume >= CAST(strftime('%Y', 'now') AS INTEGER) THEN 'now'
                WHEN b.BeginConsume > CAST(strftime('%Y', 'now') AS INTEGER) THEN 'future'
            END AS drinking_window_status
        FROM bottles b
        LEFT JOIN wines w ON b.iWine = w.iWine
        ${where}
        GROUP BY b.iWine, b.Location
        ORDER BY b.Wine, b.Vintage, b.Location
        LIMIT 200
        `;

    return db.prepare(sql).bind(...params).all();
}

export interface WineSearchFilters {
    vintage_min?: number | undefined;
    vintage_max?: number | undefined;
    country?: string | undefined;
    region?: string | undefined;
    sub_region?: string | undefined;
    appellation?: string | undefined;
    producer?: string | undefined;
    type?: string | undefined;
    varietal?: string | undefined;
    min_score?: number | undefined;
    in_drinking_window?: boolean | undefined;
    in_stock_only?: boolean | undefined;
}

export async function searchWines(db: D1Database, filters: WineSearchFilters) {
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (filters.vintage_min !== undefined) {
        conditions.push('Vintage >= ?');
        params.push(filters.vintage_min);
    }
    if (filters.vintage_max !== undefined) {
        conditions.push('Vintage <= ?');
        params.push(filters.vintage_max);
    }
    if (filters.country) {
        conditions.push('Country LIKE ?');
        params.push(`%${filters.country}%`);
    }
    if (filters.region) {
        conditions.push('Region LIKE ?');
        params.push(`%${filters.region}%`);
    }
    if (filters.sub_region) {
        conditions.push('SubRegion LIKE ?');
        params.push(`%${filters.sub_region}%`);
    }
    if (filters.appellation) {
        conditions.push('Appellation LIKE ?');
        params.push(`%${filters.appellation}%`);
    }
    if (filters.producer) {
        conditions.push('Producer LIKE ?');
        params.push(`%${filters.producer}%`);
    }
    if (filters.type) {
        conditions.push('Type LIKE ?');
        params.push(`%${filters.type}%`);
    }
    if (filters.varietal) {
        conditions.push('Varietal LIKE ?');
        params.push(`%${filters.varietal}%`);
    }
    if (filters.min_score !== undefined) {
        conditions.push('(CT >= ? OR WA >= ? OR VM >= ? OR JD >= ? OR MY >= ?)');
        params.push(filters.min_score, filters.min_score, filters.min_score, filters.min_score, filters.min_score);
    }
    if (filters.in_drinking_window === true) {
        conditions.push('BeginConsume IS NOT NULL AND EndConsume IS NOT NULL AND BeginConsume <= CAST(strftime(\'%Y\', \'now\') AS INTEGER) AND EndConsume >= CAST(strftime(\'%Y\', \'now\') AS INTEGER)');
    }
    if (filters.in_stock_only === true) {
        conditions.push('Quantity > 0');
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql = `
        SELECT Wine AS wine, Vintage AS vintage, Size AS size,
            COALESCE(Quantity, 0) AS bottles_in_cellar, COALESCE(Pending, 0) AS bottles_pending_delivery, COALESCE(Quantity, 0) + COALESCE(Pending, 0) AS bottles_total,
            Country AS country, Region AS region, SubRegion AS sub_region, Appellation AS appellation,
            Producer AS producer, Type AS type, Varietal AS varietal, Designation AS designation, Vineyard AS vineyard,
            VM AS score_vm, JD AS score_jd, WA AS score_wa, CT AS score_ct, MY AS score_my,
            BeginConsume AS begin_consume_year, EndConsume AS end_consume_year,
            CASE
                WHEN BeginConsume IS NULL OR EndConsume IS NULL THEN NULL
                WHEN EndConsume < CAST(strftime('%Y', 'now') AS INTEGER) THEN 'past'
                WHEN BeginConsume <= CAST(strftime('%Y', 'now') AS INTEGER) AND EndConsume >= CAST(strftime('%Y', 'now') AS INTEGER) THEN 'now'
                WHEN BeginConsume > CAST(strftime('%Y', 'now') AS INTEGER) THEN 'future'
            END AS drinking_window_status
        FROM wines ${where}
        ORDER BY Wine, Vintage
        LIMIT 100`;

    return db.prepare(sql).bind(...params).all();
}
