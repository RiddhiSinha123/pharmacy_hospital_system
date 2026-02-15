# 💊 Pharmacy Management System (Spring Boot)

A simple backend Pharmacy Management System built using Java Spring Boot and MySQL.

This project currently supports basic medicine inventory management features and is designed to be extended into a production-ready system in the future.

---

## 🚀 Current Features

1. ✅ Add Medicine  
   - Add medicine with:
     - Name
     - Stock
     - Expiry Date
     - Low Stock Threshold

2. ✅ Sell Medicine  
   - Reduces stock quantity
   - Prevents selling if stock is insufficient

3. ✅ Low Stock Alert  
   - Fetch medicines where stock is below a threshold

---

## 🛠️ Tech Stack

- Java 23
- Spring Boot
- Spring Data JPA
- MySQL
- Hibernate
- Maven
- IntelliJ IDEA

---

## 📂 Project Structure

src
└── main
├── java/com/example
│ ├── controller
│ ├── service
│ ├── repository
│ ├── model
│ └── PharmacyApplication.java
└── resources
└── application.properties


---

## ⚙️ Setup Instructions

### 1️⃣ Clone the Repository

```bash
git clone <your-repository-url>
cd pharmacy

CREATE DATABASE pharmacy_db;

spring.datasource.url=jdbc:mysql://localhost:3306/pharmacy_db
spring.datasource.username=root
spring.datasource.password=your_password

spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=true


4️⃣ Run the Application

From IntelliJ:

Run PharmacyApplication

Or using Maven:

mvn spring-boot:run


Application runs at:

http://localhost:8080

📡 API Endpoints
➕ Add Medicine
POST /api/medicine/add


Body:

{
  "name": "Paracetamol",
  "stock": 50,
  "expiryDate": "2026-05-10",
  "lowStockLevel": 10
}

🛒 Sell Medicine
PUT /api/medicine/sell/{id}?qty=5


Example:

PUT /api/medicine/sell/1?qty=5

⚠️ Low Stock Medicines
GET /api/medicine/low-stock?threshold=10

Future Enhancements

User authentication (Admin / Pharmacist roles)

Expiry alerts

Pagination & filtering

Invoice generation

Dashboard with analytics

Unit & Integration testing

Docker containerization

🚀 Future DevOps Plan – Blue-Green Deployment

In future versions, this project will implement Blue-Green Deployment strategy for zero-downtime releases.

Planned Architecture:

Dockerized Spring Boot application

Two identical environments:

🔵 Blue (Current Live Version)

🟢 Green (New Version)

Load balancer to switch traffic

Deployment via:

Docker Compose / Kubernetes

CI/CD pipeline (GitHub Actions)

Benefits:

Zero downtime

Easy rollback

Safer production deployments
