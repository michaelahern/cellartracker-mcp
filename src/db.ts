const WINE_COLUMNS = [
    'iWine', 'Quantity', 'Pending',
    'Size', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Color', 'Category', 'Varietal', 'Designation', 'Vineyard',
    'WA', 'VM', 'JD', 'CT', 'MY', 'BeginConsume', 'EndConsume'
];

const BOTTLE_COLUMNS = [
    'Barcode', 'iWine', 'BottleState', 'Location', 'Bin', 'Store', 'BottleCost', 'BottleCostCurrency', 'PurchaseDate', 'DeliveryDate', 'ConsumptionDate',
    'BottleSize', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Varietal', 'Designation', 'Vineyard',
    'BeginConsume', 'EndConsume'
];

const REVIEW_COLUMNS = [
    'iReview', 'iWine', 'Publication', 'ReviewDate', 'Reviewer',
    'Score', 'ReviewText', 'ReviewURL', 'BeginConsume', 'EndConsume'
];

export async function initSchema(db: D1Database) {
    await db.batch([
        db.prepare(
            'CREATE TABLE IF NOT EXISTS wines (iWine INTEGER PRIMARY KEY, Quantity INTEGER, Pending INTEGER, Size TEXT, Vintage INTEGER, Wine TEXT, Country TEXT, Region TEXT, SubRegion TEXT, Appellation TEXT, Producer TEXT, Type TEXT, Color TEXT, Category TEXT, Varietal TEXT, Designation TEXT, Vineyard TEXT, WA INTEGER, VM INTEGER, JD INTEGER, CT INTEGER, MY INTEGER, BeginConsume INTEGER, EndConsume INTEGER)'
        ),
        db.prepare(
            'CREATE TABLE IF NOT EXISTS bottles (Barcode INTEGER PRIMARY KEY, iWine INTEGER, BottleState INTEGER, Location TEXT, Bin TEXT, Store TEXT, BottleCost REAL, BottleCostCurrency TEXT, PurchaseDate TEXT, DeliveryDate TEXT, ConsumptionDate TEXT, BottleSize TEXT, Vintage INTEGER, Wine TEXT, Country TEXT, Region TEXT, SubRegion TEXT, Appellation TEXT, Producer TEXT, Type TEXT, Varietal TEXT, Designation TEXT, Vineyard TEXT, BeginConsume INTEGER, EndConsume INTEGER)'
        ),
        db.prepare(
            'CREATE TABLE IF NOT EXISTS reviews (iReview INTEGER PRIMARY KEY, iWine INTEGER, Publication TEXT, ReviewDate TEXT, Reviewer TEXT, Score INTEGER, ReviewText TEXT, ReviewURL TEXT, BeginConsume INTEGER, EndConsume INTEGER)'
        ),
        db.prepare('CREATE INDEX IF NOT EXISTS idx_bottles_wine ON bottles (iWine)'),
        db.prepare('CREATE INDEX IF NOT EXISTS idx_bottles_wine_state ON bottles (iWine, BottleState)'),
        db.prepare('CREATE INDEX IF NOT EXISTS idx_bottles_wine_state_location ON bottles (iWine, BottleState, Location, Bin)'),
        db.prepare('CREATE INDEX IF NOT EXISTS idx_bottles_state ON bottles (BottleState)'),
        db.prepare('CREATE INDEX IF NOT EXISTS idx_reviews_wine ON reviews (iWine)'),
        db.prepare('CREATE INDEX IF NOT EXISTS idx_reviews_wine_reviewdate ON reviews (iWine, ReviewDate DESC)'),
        db.prepare('CREATE INDEX IF NOT EXISTS idx_reviews_wine_reviewdate_publication ON reviews (iWine, ReviewDate DESC, Publication)'),
        db.prepare('CREATE INDEX IF NOT EXISTS idx_reviews_wine_publication_reviewdate ON reviews (iWine, Publication, ReviewDate DESC)'),
        db.prepare('CREATE INDEX IF NOT EXISTS idx_reviews_publication_wine_reviewdate ON reviews (Publication, iWine, ReviewDate DESC)')
    ]);
}

async function truncateAndInsert(db: D1Database, table: string, columns: string[], rows: Record<string, unknown>[]): Promise<string | null> {
    try {
        const insertSQL = `INSERT OR REPLACE INTO ${table} (${columns.join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`;

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
        return null;
    }
    catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`Error inserting into ${table}: ${message}`);
        return `${table}: ${message}`;
    }
}

export async function truncateAndInsertWines(db: D1Database, rows: Record<string, unknown>[]) {
    return truncateAndInsert(db, 'wines', WINE_COLUMNS, rows);
}

export async function truncateAndInsertBottles(db: D1Database, rows: Record<string, unknown>[]) {
    return truncateAndInsert(db, 'bottles', BOTTLE_COLUMNS, rows);
}

export async function truncateAndInsertReviews(db: D1Database, rows: Record<string, unknown>[]) {
    return truncateAndInsert(db, 'reviews', REVIEW_COLUMNS, rows);
}

export async function getCellarStats(db: D1Database) {
    const results = await db.batch<Record<string, unknown>>([
        db.prepare(`
            SELECT
                COALESCE(SUM(Quantity), 0) AS bottles_in_stock,
                COALESCE(SUM(Pending), 0) AS bottles_pending_delivery,
                COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_in_cellar,
                COUNT(*) AS unique_wines_in_cellar,
                '$' || CAST(CAST(ROUND((SELECT SUM(BottleCost) FROM bottles WHERE (BottleState = 1 OR BottleState = -1))) AS INTEGER) AS TEXT) AS total_cellar_value,
                (SELECT COUNT(*) FROM bottles WHERE BottleState = 0) AS bottles_consumed,
                (SELECT COUNT(*) FROM bottles) AS bottles_purchased
            FROM wines
        `),
        db.prepare(`
            SELECT Location AS location, COUNT(*) AS bottles_in_stock
            FROM bottles
            WHERE BottleState = 1 AND Location IS NOT NULL AND Location != ''
            GROUP BY Location
            ORDER BY bottles_in_stock DESC
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
                COALESCE(SUM(Quantity), 0) AS bottles_in_stock,
                COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_in_cellar
            FROM wines
            WHERE BeginConsume IS NOT NULL AND EndConsume IS NOT NULL
            GROUP BY window
            ORDER BY window ASC
        `),
        db.prepare(`
            SELECT Type AS type, COALESCE(SUM(Quantity), 0) AS bottles_in_stock, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_in_cellar
            FROM wines
            WHERE Type IS NOT NULL AND Type != '' AND Type != 'Unknown'
            GROUP BY Type
            ORDER BY bottles_in_cellar DESC
            LIMIT 100
        `),
        db.prepare(`
            SELECT w.Varietal AS varietal, COALESCE(SUM(w.Quantity), 0) AS bottles_in_stock, COALESCE(SUM(w.Quantity), 0) + COALESCE(SUM(w.Pending), 0) AS bottles_in_cellar,
                '$' || CAST(CAST(ROUND(SUM(bc.cost_sum) * 1.0 / SUM(bc.bottle_count)) AS INTEGER) AS TEXT) AS avg_bottle_cost,
                ROUND(AVG(CASE WHEN w.JD IS NOT NULL THEN w.JD END), 1) AS avg_score_jd, ROUND(AVG(CASE WHEN twp.Score IS NOT NULL THEN twp.Score END), 1) AS avg_score_twp,
                ROUND(AVG(CASE WHEN w.VM IS NOT NULL THEN w.VM END), 1) AS avg_score_vm, ROUND(AVG(CASE WHEN w.WA IS NOT NULL THEN w.WA END), 1) AS avg_score_wa
            FROM wines w
            LEFT JOIN (SELECT iWine, Score, ROW_NUMBER() OVER (PARTITION BY iWine ORDER BY ReviewDate DESC) AS rn FROM reviews WHERE Publication = 'The Wine Palate') twp ON w.iWine = twp.iWine AND twp.rn = 1
            LEFT JOIN (SELECT iWine, SUM(BottleCost) AS cost_sum, COUNT(BottleCost) AS bottle_count FROM bottles WHERE BottleState IN (-1, 1) AND BottleCost IS NOT NULL AND BottleCost != 0 GROUP BY iWine) bc ON w.iWine = bc.iWine
            WHERE w.Varietal IS NOT NULL AND w.Varietal != '' AND w.Varietal != 'Unknown'
            GROUP BY w.Varietal
            ORDER BY bottles_in_cellar DESC
            LIMIT 100
        `),
        db.prepare(`
            SELECT w.Producer AS producer, COALESCE(SUM(w.Quantity), 0) AS bottles_in_stock, COALESCE(SUM(w.Quantity), 0) + COALESCE(SUM(w.Pending), 0) AS bottles_in_cellar,
                '$' || CAST(CAST(ROUND(SUM(bc.cost_sum) * 1.0 / SUM(bc.bottle_count)) AS INTEGER) AS TEXT) AS avg_bottle_cost,
                ROUND(AVG(CASE WHEN w.JD IS NOT NULL THEN w.JD END), 1) AS avg_score_jd, ROUND(AVG(CASE WHEN twp.Score IS NOT NULL THEN twp.Score END), 1) AS avg_score_twp,
                ROUND(AVG(CASE WHEN w.VM IS NOT NULL THEN w.VM END), 1) AS avg_score_vm, ROUND(AVG(CASE WHEN w.WA IS NOT NULL THEN w.WA END), 1) AS avg_score_wa
            FROM wines w
            LEFT JOIN (SELECT iWine, Score, ROW_NUMBER() OVER (PARTITION BY iWine ORDER BY ReviewDate DESC) AS rn FROM reviews WHERE Publication = 'The Wine Palate') twp ON w.iWine = twp.iWine AND twp.rn = 1
            LEFT JOIN (SELECT iWine, SUM(BottleCost) AS cost_sum, COUNT(BottleCost) AS bottle_count FROM bottles WHERE BottleState IN (-1, 1) AND BottleCost IS NOT NULL AND BottleCost != 0 GROUP BY iWine) bc ON w.iWine = bc.iWine
            WHERE w.Producer IS NOT NULL AND w.Producer != '' AND w.Producer != 'Unknown'
            GROUP BY w.Producer
            ORDER BY bottles_in_cellar DESC
            LIMIT 100
        `),
        db.prepare(`
            SELECT Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_stock, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_in_cellar
            FROM wines
            WHERE Country IS NOT NULL AND Country != '' AND Country != 'Unknown'
            GROUP BY Country
            ORDER BY bottles_in_cellar DESC
            LIMIT 50
        `),
        db.prepare(`
            SELECT Region AS region, Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_stock, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_in_cellar
            FROM wines
            WHERE Region IS NOT NULL AND Region != '' AND Region != 'Unknown'
            GROUP BY Region
            ORDER BY bottles_in_cellar DESC
            LIMIT 50
        `),
        db.prepare(`
            SELECT SubRegion AS sub_region, Region AS region, Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_stock, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_in_cellar
            FROM wines
            WHERE SubRegion IS NOT NULL AND SubRegion != '' AND SubRegion != 'Unknown'
            GROUP BY SubRegion
            ORDER BY bottles_in_cellar DESC
            LIMIT 50
        `),
        db.prepare(`
            SELECT Appellation AS appellation, SubRegion AS sub_region, Region AS region, Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_stock, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_in_cellar
            FROM wines
            WHERE Appellation IS NOT NULL AND Appellation != '' AND Appellation != 'Unknown'
            GROUP BY Appellation
            ORDER BY bottles_in_cellar DESC
            LIMIT 50
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
    type?: string | undefined;
    varietal?: string | undefined;
    producer?: string | undefined;
    country?: string | undefined;
    region?: string | undefined;
    sub_region?: string | undefined;
    appellation?: string | undefined;
    designation?: string | undefined;
    vineyard?: string | undefined;
    min_score?: number | undefined;
    in_drinking_window?: boolean | undefined;
    location?: string | undefined;
    bottle_state_in_cellar?: boolean | undefined;
    bottle_state_consumed?: boolean | undefined;
    bottle_state_pending_delivery?: boolean | undefined;
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
    if (filters.type) {
        conditions.push('b.Type LIKE ?');
        params.push(`%${filters.type}%`);
    }
    if (filters.varietal) {
        conditions.push('b.Varietal LIKE ?');
        params.push(`%${filters.varietal}%`);
    }
    if (filters.producer) {
        conditions.push('b.Producer LIKE ?');
        params.push(`%${filters.producer}%`);
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
    if (filters.designation) {
        conditions.push('b.Designation LIKE ?');
        params.push(`%${filters.designation}%`);
    }
    if (filters.vineyard) {
        conditions.push('b.Vineyard LIKE ?');
        params.push(`%${filters.vineyard}%`);
    }
    if (filters.min_score !== undefined) {
        conditions.push('(w.JD >= ? OR twp.Score >= ? OR w.VM >= ? OR wa.Score >= ?)');
        params.push(filters.min_score, filters.min_score, filters.min_score, filters.min_score);
    }
    if (filters.in_drinking_window === true) {
        conditions.push('b.BeginConsume IS NOT NULL AND b.EndConsume IS NOT NULL AND b.BeginConsume <= CAST(strftime(\'%Y\', \'now\') AS INTEGER) AND b.EndConsume >= CAST(strftime(\'%Y\', \'now\') AS INTEGER)');
    }
    if (filters.location) {
        conditions.push('(b.Location LIKE ?)');
        params.push(`%${filters.location}%`);
    }

    const states: number[] = [];
    if (filters.bottle_state_in_cellar !== false) states.push(1);
    if (filters.bottle_state_consumed === true) states.push(0);
    if (filters.bottle_state_pending_delivery === true) states.push(-1);
    conditions.push(`b.BottleState IN (${states.map(() => '?').join(', ')})`);
    params.push(...states);

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql = `
        SELECT b.Wine AS wine, b.Vintage AS vintage, b.BottleSize AS size,
            CASE b.BottleState WHEN 1 THEN 'in_cellar' WHEN 0 THEN 'consumed' WHEN -1 THEN 'pending_delivery' END AS bottle_state,
            CASE WHEN (b.BottleState <= 0 OR b.Location = 'none') THEN NULL ELSE b.Location END AS location,
            (SELECT GROUP_CONCAT(bin_summary, '; ') FROM (SELECT bb.Bin || ' (x' || COUNT(*) || ')' AS bin_summary FROM bottles bb WHERE bb.iWine = b.iWine AND bb.BottleState = b.BottleState AND bb.Location = b.Location AND bb.BottleState = 1 GROUP BY bb.Bin)) AS bins,
            CASE WHEN b.BottleState = 1 THEN COUNT(*) ELSE NULL END AS bottles_in_stock_this_location,
            CASE WHEN b.BottleState = 0 THEN COUNT(*) ELSE NULL END AS bottles_consumed, CASE WHEN b.BottleState = 0 THEN MAX(b.ConsumptionDate) ELSE NULL END AS last_consumed_date,
            CASE WHEN b.BottleState = -1 THEN COUNT(*) ELSE NULL END AS bottles_pending_delivery, CASE WHEN b.BottleState = -1 THEN MIN(b.DeliveryDate) ELSE NULL END AS next_delivery_date, 
            COALESCE(w.Quantity, 0) + COALESCE(w.Pending, 0) AS bottles_in_cellar,
            CASE
                WHEN ROUND(AVG(b.BottleCost)) IS NULL THEN NULL
                ELSE '$' || CAST(CAST(ROUND(AVG(b.BottleCost)) AS INTEGER) AS TEXT)
            END AS avg_bottle_cost,
            b.Country AS country, b.Region AS region, b.SubRegion AS sub_region, b.Appellation AS appellation,
            b.Producer AS producer, b.Type AS type, b.Varietal AS varietal, b.Designation AS designation, b.Vineyard AS vineyard,
            w.JD AS score_jd, twp.Score AS score_twp, twp.ReviewText AS review_twp, w.VM AS score_vm, wa.Score AS score_wa, wa.ReviewText AS review_wa,
            b.BeginConsume AS begin_consume_year, b.EndConsume AS end_consume_year,
            CASE
                WHEN b.BeginConsume IS NULL OR b.EndConsume IS NULL THEN NULL
                WHEN b.EndConsume < CAST(strftime('%Y', 'now') AS INTEGER) THEN 'past'
                WHEN b.BeginConsume <= CAST(strftime('%Y', 'now') AS INTEGER) AND b.EndConsume >= CAST(strftime('%Y', 'now') AS INTEGER) THEN 'now'
                WHEN b.BeginConsume > CAST(strftime('%Y', 'now') AS INTEGER) THEN 'future'
            END AS drinking_window_status
        FROM bottles b
        LEFT JOIN wines w ON b.iWine = w.iWine
        LEFT JOIN (SELECT iWine, Score, ReviewText, ROW_NUMBER() OVER (PARTITION BY iWine ORDER BY ReviewDate DESC) AS rn FROM reviews WHERE Publication = 'The Wine Palate') twp ON w.iWine = twp.iWine AND twp.rn = 1
        LEFT JOIN (SELECT iWine, Score, ReviewText, ROW_NUMBER() OVER (PARTITION BY iWine ORDER BY ReviewDate DESC) AS rn FROM reviews WHERE Publication = 'Wine Advocate') wa ON w.iWine = wa.iWine AND wa.rn = 1
        ${where}
        GROUP BY b.iWine, b.BottleState, location
        ORDER BY b.Wine, b.Vintage, b.BottleState, location
        LIMIT 200
        `;

    return db.prepare(sql).bind(...params).all();
}

export interface WineSearchFilters {
    vintage_min?: number | undefined;
    vintage_max?: number | undefined;
    type?: string | undefined;
    varietal?: string | undefined;
    producer?: string | undefined;
    country?: string | undefined;
    region?: string | undefined;
    sub_region?: string | undefined;
    appellation?: string | undefined;
    designation?: string | undefined;
    vineyard?: string | undefined;
    min_score?: number | undefined;
    in_drinking_window?: boolean | undefined;
}

export async function searchWines(db: D1Database, filters: WineSearchFilters) {
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (filters.vintage_min !== undefined) {
        conditions.push('w.Vintage >= ?');
        params.push(filters.vintage_min);
    }
    if (filters.vintage_max !== undefined) {
        conditions.push('w.Vintage <= ?');
        params.push(filters.vintage_max);
    }
    if (filters.type) {
        conditions.push('w.Type LIKE ?');
        params.push(`%${filters.type}%`);
    }
    if (filters.varietal) {
        conditions.push('w.Varietal LIKE ?');
        params.push(`%${filters.varietal}%`);
    }
    if (filters.producer) {
        conditions.push('w.Producer LIKE ?');
        params.push(`%${filters.producer}%`);
    }
    if (filters.country) {
        conditions.push('w.Country LIKE ?');
        params.push(`%${filters.country}%`);
    }
    if (filters.region) {
        conditions.push('w.Region LIKE ?');
        params.push(`%${filters.region}%`);
    }
    if (filters.sub_region) {
        conditions.push('w.SubRegion LIKE ?');
        params.push(`%${filters.sub_region}%`);
    }
    if (filters.appellation) {
        conditions.push('w.Appellation LIKE ?');
        params.push(`%${filters.appellation}%`);
    }
    if (filters.designation) {
        conditions.push('w.Designation LIKE ?');
        params.push(`%${filters.designation}%`);
    }
    if (filters.vineyard) {
        conditions.push('w.Vineyard LIKE ?');
        params.push(`%${filters.vineyard}%`);
    }
    if (filters.min_score !== undefined) {
        conditions.push('(w.JD >= ? OR twp.Score >= ? OR w.VM >= ? OR wa.Score >= ?)');
        params.push(filters.min_score, filters.min_score, filters.min_score, filters.min_score);
    }
    if (filters.in_drinking_window === true) {
        conditions.push('w.BeginConsume IS NOT NULL AND w.EndConsume IS NOT NULL AND w.BeginConsume <= CAST(strftime(\'%Y\', \'now\') AS INTEGER) AND w.EndConsume >= CAST(strftime(\'%Y\', \'now\') AS INTEGER)');
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql = `
        SELECT w.Wine AS wine, w.Vintage AS vintage, w.Size AS size,
            COALESCE(w.Quantity, 0) AS bottles_in_stock, COALESCE(w.Pending, 0) AS bottles_pending_delivery, COALESCE(w.Quantity, 0) + COALESCE(w.Pending, 0) AS bottles_in_cellar,
            (SELECT COUNT(*) FROM bottles b WHERE b.iWine = w.iWine AND b.BottleState = 0) AS bottles_consumed,
            w.Country AS country, w.Region AS region, w.SubRegion AS sub_region, w.Appellation AS appellation,
            w.Producer AS producer, w.Type AS type, w.Varietal AS varietal, w.Designation AS designation, w.Vineyard AS vineyard,
            '$' || CAST(CAST(ROUND(bc.avg_bottle_cost) AS INTEGER) AS TEXT) AS avg_bottle_cost,
            w.JD AS score_jd, twp.Score AS score_twp, twp.ReviewText AS review_twp, w.VM AS score_vm, wa.Score AS score_wa, wa.ReviewText AS review_wa,
            w.BeginConsume AS begin_consume_year, w.EndConsume AS end_consume_year,
            CASE
                WHEN w.BeginConsume IS NULL OR w.EndConsume IS NULL THEN NULL
                WHEN w.EndConsume < CAST(strftime('%Y', 'now') AS INTEGER) THEN 'past'
                WHEN w.BeginConsume <= CAST(strftime('%Y', 'now') AS INTEGER) AND w.EndConsume >= CAST(strftime('%Y', 'now') AS INTEGER) THEN 'now'
                WHEN w.BeginConsume > CAST(strftime('%Y', 'now') AS INTEGER) THEN 'future'
            END AS drinking_window_status
        FROM wines w
        LEFT JOIN (SELECT iWine, AVG(BottleCost) AS avg_bottle_cost FROM bottles WHERE BottleState IN (-1, 1) AND BottleCost IS NOT NULL AND BottleCost != 0 GROUP BY iWine) bc ON w.iWine = bc.iWine
        LEFT JOIN (SELECT iWine, Score, ReviewText, ROW_NUMBER() OVER (PARTITION BY iWine ORDER BY ReviewDate DESC) AS rn FROM reviews WHERE Publication = 'The Wine Palate') twp ON w.iWine = twp.iWine AND twp.rn = 1
        LEFT JOIN (SELECT iWine, Score, ReviewText, ROW_NUMBER() OVER (PARTITION BY iWine ORDER BY ReviewDate DESC) AS rn FROM reviews WHERE Publication = 'Wine Advocate') wa ON w.iWine = wa.iWine AND wa.rn = 1
        ${where}
        ORDER BY w.Wine, w.Vintage
        LIMIT 100
        `;

    return db.prepare(sql).bind(...params).all();
}
