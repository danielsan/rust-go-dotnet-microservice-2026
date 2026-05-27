use actix_web::{delete, get, post, web, App, HttpResponse, HttpServer, Responder};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;
use validator::Validate;

#[derive(Serialize, sqlx::FromRow)]
struct User {
    id: Uuid,
    name: String,
    email: String,
    created_at: DateTime<Utc>,
}

#[derive(Deserialize, Validate)]
struct CreateUserRequest {
    #[validate(length(min = 1, max = 100))]
    name: String,
    #[validate(email)]
    email: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[get("/health")]
async fn health() -> impl Responder {
    web::Json(HealthResponse { status: "ok" })
}

#[post("/users")]
async fn create_user(pool: web::Data<PgPool>, payload: web::Json<CreateUserRequest>) -> impl Responder {
    let req = payload.into_inner();
    if let Err(e) = req.validate() {
        return HttpResponse::UnprocessableEntity().json(ErrorResponse { error: e.to_string() });
    }

    let res = sqlx::query_as::<_, User>(
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email, created_at",
    )
    .bind(&req.name)
    .bind(&req.email)
    .fetch_one(pool.get_ref())
    .await;

    match res {
        Ok(user) => {
            // background non-blocking job
            let email = user.email.clone();
            actix_web::rt::spawn(async move {
                println!("NOTIFY: email sent to {} at {}", email, Utc::now().to_rfc3339());
            });
            HttpResponse::Created().json(user)
        }
        Err(e) => HttpResponse::InternalServerError().json(ErrorResponse { error: e.to_string() }),
    }
}

#[get("/users/{id}")]
async fn get_user(pool: web::Data<PgPool>, path: web::Path<String>) -> impl Responder {
    let id = match Uuid::parse_str(&path.into_inner()) {
        Ok(u) => u,
        Err(_) => return HttpResponse::NotFound().json(ErrorResponse{ error: "invalid uuid".into() }),
    };

    let res = sqlx::query_as::<_, User>(
        "SELECT id, name, email, created_at FROM users WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool.get_ref())
    .await;

    match res {
        Ok(user) => HttpResponse::Ok().json(user),
        Err(sqlx::Error::RowNotFound) => HttpResponse::NotFound().json(ErrorResponse{ error: "user not found".into() }),
        Err(e) => HttpResponse::InternalServerError().json(ErrorResponse{ error: e.to_string() }),
    }
}

#[get("/users")]
async fn list_users(pool: web::Data<PgPool>) -> impl Responder {
    let res = sqlx::query_as::<_, User>(
        "SELECT id, name, email, created_at FROM users ORDER BY created_at DESC LIMIT 100",
    )
    .fetch_all(pool.get_ref())
    .await;

    match res {
        Ok(users) => HttpResponse::Ok().json(users),
        Err(e) => HttpResponse::InternalServerError().json(ErrorResponse{ error: e.to_string() }),
    }
}

#[delete("/users/{id}")]
async fn delete_user(pool: web::Data<PgPool>, path: web::Path<String>) -> impl Responder {
    let id = match Uuid::parse_str(&path.into_inner()) {
        Ok(u) => u,
        Err(_) => return HttpResponse::NotFound().json(ErrorResponse{ error: "invalid uuid".into() }),
    };

    let res = sqlx::query("DELETE FROM users WHERE id = $1").bind(id).execute(pool.get_ref()).await;
    match res {
        Ok(r) => {
            if r.rows_affected() == 0 {
                HttpResponse::NotFound().json(ErrorResponse{ error: "user not found".into() })
            } else {
                HttpResponse::NoContent().finish()
            }
        }
        Err(e) => HttpResponse::InternalServerError().json(ErrorResponse{ error: e.to_string() }),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://bench:bench@127.0.0.1:5432/bench".into());
    let port = std::env::var("PORT").unwrap_or_else(|_| "3001".into());

    // Pool configuration
    let pool = PgPool::connect(&database_url).await.expect("Failed to create pool");

    // Ensure schema exists
    if let Err(e) = sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(&pool)
    .await
    {
        eprintln!("Failed to run migrations: {}", e);
        std::process::exit(1);
    }

    let pool_data = web::Data::new(pool);

    println!("rust-actix listening on :{}", port);

    HttpServer::new(move || {
        App::new()
            .app_data(pool_data.clone())
            .service(health)
            .service(create_user)
            .service(get_user)
            .service(list_users)
            .service(delete_user)
    })
    .bind(format!("0.0.0.0:{}", port))?
    .run()
    .await
}
