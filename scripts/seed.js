const fs = require('fs');

const generateData = () => {
    const data = [];
    for (let i = 1; i <= 20; i++) {
        data.push({
            order_id: i.toString(),
            customer_id: ((i % 5) + 1).toString(),
            status: "PLACED",
            items: [
                {
                    product_id: "1",
                    quantity: (i % 3) + 1,
                    unit_price: 45.00
                },
                {
                    product_id: "2",
                    quantity: (i % 2) + 1,
                    unit_price: 110.00
                }
            ],
            total_amount: ((i % 3) + 1) * 45.00 + ((i % 2) + 1) * 110.00,
            currency: "BRL",
            created_at: new Date(new Date('2026-03-19T11:00:00Z').getTime() + i * 60000).toISOString()
        });
    }
    return data;
};

fs.writeFileSync('data/seed_data.json', JSON.stringify(generateData(), null, 2));
console.log('Seed data generated at data/seed_data.json');
