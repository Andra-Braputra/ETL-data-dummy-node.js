const sqlite3 = require("sqlite3").verbose();

const customers = [
  { id: 101, name: "Alice", gender: "P" },
  { id: 102, name: "Bob", gender: "L" },
  { id: 103, name: "Charlie", gender: "L" },
  { id: 104, name: "David", gender: "M" },
];

const movies = [
  { id: 201, title: "Inception", price: 5.0 },
  { id: 202, title: "The Dark Knight", price: 4.5 },
  { id: 203, title: "Interstellar", price: 6.0 },
  { id: 204, title: "Tenet", price: 5.5 },
  { id: 205, title: "Dunkirk", price: 4.0 },
];

const transactions = [
  {
    id: 1,
    date: "2023-10-01 10:00:00",
    location: "Cinema A",
    customer_id: 101,
  },
  {
    id: 2,
    date: "2023-10-01 12:30:00",
    location: "Cinema B",
    customer_id: 102,
  },
  {
    id: 3,
    date: "2023-10-02 15:00:00",
    location: "Cinema A",
    customer_id: 101,
  },
  { id: 4, date: "2023-10-03 11:00:00", location: "Online", customer_id: 103 },
];

const transaction_items = [
  { movie_id: 201, transaction_id: 1, price: 5.0, discount: 0.5 },
  { movie_id: 202, transaction_id: 1, price: 4.5, discount: 0.0 },
  { movie_id: 203, transaction_id: 2, price: 6.0, discount: 0.5 },
  { movie_id: 202, transaction_id: 2, price: 4.5, discount: 0.0 },
  { movie_id: 204, transaction_id: 3, price: 5.5, discount: 0.25 },
  { movie_id: 205, transaction_id: 3, price: 4.0, discount: 0.0 },
  { movie_id: 201, transaction_id: 4, price: 5.0, discount: 0.75 },
];

class StarSchemaETL {
  constructor() {
    this.db = new sqlite3.Database(":memory:");
  }

  transformDateDimension() {
    const dateSet = new Set();

    transactions.forEach((transaction) => {
      const date = new Date(transaction.date);
      const dateKey = date.toISOString().split("T")[0];
      dateSet.add(dateKey);
    });

    return Array.from(dateSet).map((dateStr) => {
      const date = new Date(dateStr);
      return {
        date_key: dateStr.replace(/-/g, ""),
        full_date: dateStr,
        day: date.getDate(),
        month: date.getMonth() + 1,
        quarter: Math.floor((date.getMonth() + 3) / 3),
        year: date.getFullYear(),
        day_of_week: date.getDay(),
        day_name: date.toLocaleDateString("en-US", { weekday: "long" }),
        month_name: date.toLocaleDateString("en-US", { month: "long" }),
        is_weekend: [0, 6].includes(date.getDay()) ? 1 : 0,
      };
    });
  }

  transformCustomerDimension() {
    return customers.map((customer) => {
      let gender;
      const originalGender = customer.gender;

      if (originalGender === "F" || originalGender === "M") {
        gender = originalGender;
      } else {
        gender = originalGender === "P" ? "F" : "M";
      }

      return {
        customer_key: customer.id,
        customer_id: customer.id,
        customer_name: customer.name.trim(),
        gender: gender,
      };
    });
  }

  transformMovieDimension() {
    return movies.map((movie) => ({
      movie_key: movie.id,
      movie_id: movie.id,
      movie_title: movie.title.trim(),
      base_price: movie.price,
      price_category: this.getPriceCategory(movie.price),
    }));
  }

  getPriceCategory(price) {
    if (price < 4.5) return "Budget";
    if (price < 5.5) return "Standard";
    return "Premium";
  }

  transformLocationDimension() {
    const locations = [...new Set(transactions.map((t) => t.location))];
    return locations.map((location, index) => ({
      location_key: index + 1,
      location_name: location,
      location_type: location === "Online" ? "Online" : "Physical",
      region: this.getRegion(location),
    }));
  }

  getRegion(location) {
    const regions = {
      "Cinema A": "North Region",
      "Cinema B": "South Region",
      Online: "Online",
    };
    return regions[location] || "Unknown";
  }

  transformFactSales() {
    return transaction_items.map((item) => {
      const transaction = transactions.find(
        (t) => t.id === item.transaction_id
      );
      const movie = movies.find((m) => m.id === item.movie_id);
      const customer = customers.find((c) => c.id === transaction.customer_id);
      const transactionDate = new Date(transaction.date);
      const dateKey = transactionDate
        .toISOString()
        .split("T")[0]
        .replace(/-/g, "");

      const locationDim = this.transformLocationDimension().find(
        (l) => l.location_name === transaction.location
      );

      return {
        date_key: dateKey,
        customer_key: customer.id,
        movie_key: movie.id,
        location_key: locationDim.location_key,
        transaction_id: transaction.id,
        quantity: 1,
        base_price: item.price,
        discount_amount: item.discount,
        final_price: parseFloat((item.price - item.discount).toFixed(2)),
        discount_percentage: parseFloat(
          ((item.discount / item.price) * 100).toFixed(2)
        ),
        profit_margin: parseFloat(
          (((item.price - item.discount) / item.price) * 100).toFixed(2)
        ),
      };
    });
  }

  async createStarSchemaTables() {
    return new Promise((resolve, reject) => {
      this.db.serialize(() => {
        this.db.run(`CREATE TABLE dim_date (
          date_key INTEGER PRIMARY KEY,
          full_date TEXT,
          day INTEGER,
          month INTEGER,
          quarter INTEGER,
          year INTEGER,
          day_of_week INTEGER,
          day_name TEXT,
          month_name TEXT,
          is_weekend INTEGER
        )`);

        this.db.run(`CREATE TABLE dim_customer (
          customer_key INTEGER PRIMARY KEY,
          customer_id INTEGER,
          customer_name TEXT,
          gender TEXT
        )`);

        this.db.run(`CREATE TABLE dim_movie (
          movie_key INTEGER PRIMARY KEY,
          movie_id INTEGER,
          movie_title TEXT,
          base_price REAL,
          price_category TEXT
        )`);

        this.db.run(`CREATE TABLE dim_location (
          location_key INTEGER PRIMARY KEY,
          location_name TEXT,
          location_type TEXT,
          region TEXT
        )`);

        this.db.run(`CREATE TABLE fact_sales (
          sales_id INTEGER PRIMARY KEY AUTOINCREMENT,
          date_key INTEGER,
          customer_key INTEGER,
          movie_key INTEGER,
          location_key INTEGER,
          transaction_id INTEGER,
          quantity INTEGER,
          base_price REAL,
          discount_amount REAL,
          final_price REAL,
          discount_percentage REAL,
          profit_margin REAL,
          FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
          FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
          FOREIGN KEY (movie_key) REFERENCES dim_movie(movie_key),
          FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
        )`);

        resolve();
      });
    });
  }

  async loadStarSchemaData() {
    const dimDate = this.transformDateDimension();
    const dimCustomer = this.transformCustomerDimension();
    const dimMovie = this.transformMovieDimension();
    const dimLocation = this.transformLocationDimension();
    const factSales = this.transformFactSales();

    for (const date of dimDate) {
      await this.insertDateDimension(date);
    }

    for (const customer of dimCustomer) {
      await this.insertCustomerDimension(customer);
    }

    for (const movie of dimMovie) {
      await this.insertMovieDimension(movie);
    }

    for (const location of dimLocation) {
      await this.insertLocationDimension(location);
    }

    for (const sale of factSales) {
      await this.insertFactSales(sale);
    }
  }

  insertDateDimension(date) {
    return new Promise((resolve, reject) => {
      this.db.run(
        `INSERT INTO dim_date (date_key, full_date, day, month, quarter, year, day_of_week, day_name, month_name, is_weekend) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          date.date_key,
          date.full_date,
          date.day,
          date.month,
          date.quarter,
          date.year,
          date.day_of_week,
          date.day_name,
          date.month_name,
          date.is_weekend,
        ],
        function (err) {
          err ? reject(err) : resolve();
        }
      );
    });
  }

  insertCustomerDimension(customer) {
    return new Promise((resolve, reject) => {
      this.db.run(
        `INSERT INTO dim_customer (customer_key, customer_id, customer_name, gender) 
         VALUES (?, ?, ?, ?)`,
        [
          customer.customer_key,
          customer.customer_id,
          customer.customer_name,
          customer.gender,
        ],
        function (err) {
          err ? reject(err) : resolve();
        }
      );
    });
  }

  insertMovieDimension(movie) {
    return new Promise((resolve, reject) => {
      this.db.run(
        `INSERT INTO dim_movie (movie_key, movie_id, movie_title, base_price, price_category) 
         VALUES (?, ?, ?, ?, ?)`,
        [
          movie.movie_key,
          movie.movie_id,
          movie.movie_title,
          movie.base_price,
          movie.price_category,
        ],
        function (err) {
          err ? reject(err) : resolve();
        }
      );
    });
  }

  insertLocationDimension(location) {
    return new Promise((resolve, reject) => {
      this.db.run(
        `INSERT INTO dim_location (location_key, location_name, location_type, region) 
         VALUES (?, ?, ?, ?)`,
        [
          location.location_key,
          location.location_name,
          location.location_type,
          location.region,
        ],
        function (err) {
          err ? reject(err) : resolve();
        }
      );
    });
  }

  insertFactSales(sale) {
    return new Promise((resolve, reject) => {
      this.db.run(
        `INSERT INTO fact_sales (date_key, customer_key, movie_key, location_key, transaction_id, quantity, base_price, discount_amount, final_price, discount_percentage, profit_margin) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          sale.date_key,
          sale.customer_key,
          sale.movie_key,
          sale.location_key,
          sale.transaction_id,
          sale.quantity,
          sale.base_price,
          sale.discount_amount,
          sale.final_price,
          sale.discount_percentage,
          sale.profit_margin,
        ],
        function (err) {
          err ? reject(err) : resolve();
        }
      );
    });
  }

  async displayAllTables() {
    console.log("1. DATE DIMENSION (dim_date):");
    const dateData = await this.query(
      "SELECT * FROM dim_date ORDER BY date_key"
    );
    console.table(dateData);

    console.log("\n2. CUSTOMER DIMENSION (dim_customer):");
    const customerData = await this.query(
      "SELECT * FROM dim_customer ORDER BY customer_key"
    );
    console.table(customerData);

    console.log("\n3. MOVIE DIMENSION (dim_movie):");
    const movieData = await this.query(
      "SELECT * FROM dim_movie ORDER BY movie_key"
    );
    console.table(movieData);

    console.log("\n4. LOCATION DIMENSION (dim_location):");
    const locationData = await this.query(
      "SELECT * FROM dim_location ORDER BY location_key"
    );
    console.table(locationData);

    console.log("\n5. ALL FACT SALES TRANSACTIONS (fact_sales):");
    const factSalesData = await this.query(`
      SELECT 
        fs.sales_id,
        dd.full_date,
        dc.customer_name as Name,
        dm.movie_title,
        dl.location_name,
        fs.quantity,
        fs.base_price as Price,
        fs.final_price as Total,
        fs.discount_percentage,
        fs.profit_margin as profit
      FROM fact_sales fs
      JOIN dim_date dd ON fs.date_key = dd.date_key
      JOIN dim_customer dc ON fs.customer_key = dc.customer_key
      JOIN dim_movie dm ON fs.movie_key = dm.movie_key
      JOIN dim_location dl ON fs.location_key = dl.location_key
      ORDER BY fs.sales_id
    `);
    console.table(factSalesData);
  }

  query(sql) {
    return new Promise((resolve, reject) => {
      this.db.all(sql, [], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });
  }

  async runETL() {
    try {
      console.log("Starting ETL...");

      await this.createStarSchemaTables();
      console.log("tables created");

      await this.loadStarSchemaData();
      console.log("Data loaded");

      await this.displayAllTables();
    } catch (error) {
      console.error("ETL process failed:", error);
    } finally {
      this.db.close();
    }
  }
}

const starSchemaETL = new StarSchemaETL();
starSchemaETL.runETL();
