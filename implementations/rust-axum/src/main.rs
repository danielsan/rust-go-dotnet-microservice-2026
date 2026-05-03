use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::task;
use tokio_postgres::NoTls;
use tracing::{debug, info};
use uuid::Uuid;
use validator::Validate;

#[derive(Clone)]
struct AppState {
    pool: Pool,
}

#[derive(Debug, Serialize)]
struct User {
    id: Uuid,
    name: String,
    email: String,
    created_at: DateTime<Utc>,
}

fn row_to_user(row: &tokio_postgres::Row) -> User {
    User {
        id: row.get(0),
        name: row.get(1),
        email: row.get(2),
        created_at: row.get(3),
    }
}

#[derive(Debug, Deserialize, Validate)]
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
    error: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn create_user(
    State(state): State<AppState>,
    Json(payload): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<User>), (StatusCode, Json<ErrorResponse>)> {
    if let Err(e) = payload.validate() {
        debug!(error = %e, "validation failed");
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ErrorResponse {
                error: "validation failed",
            }),
        ));
    }

    let client = state.pool.get().await.map_err(|e| {
        debug!(error = %e, "db pool error");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "internal server error",
            }),
        )
    })?;

    let row = client
        .query_one(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email, created_at",
            &[&payload.name, &payload.email],
        )
        .await
        .map_err(|e| {
            debug!(error = %e, "db insert failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "internal server error",
                }),
            )
        })?;

    let user = row_to_user(&row);

    let email = user.email.clone();
    task::spawn(async move {
        let ts = Utc::now();
        debug!(%email, %ts, "NOTIFY: email sent");
    });

    Ok((StatusCode::CREATED, Json(user)))
}

async fn get_user(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<User>, (StatusCode, Json<ErrorResponse>)> {
    let client = state.pool.get().await.map_err(|e| {
        debug!(error = %e, "db pool error");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "internal server error",
            }),
        )
    })?;

    let row = client
        .query_opt(
            "SELECT id, name, email, created_at FROM users WHERE id = $1",
            &[&id],
        )
        .await
        .map_err(|e| {
            debug!(error = %e, %id, "db select failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "internal server error",
                }),
            )
        })?;

    match row {
        Some(r) => Ok(Json(row_to_user(&r))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "user not found",
            }),
        )),
    }
}

async fn list_users(
    State(state): State<AppState>,
) -> Result<Json<Vec<User>>, (StatusCode, Json<ErrorResponse>)> {
    let client = state.pool.get().await.map_err(|e| {
        debug!(error = %e, "db pool error");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "internal server error",
            }),
        )
    })?;

    let rows = client
        .query(
            "SELECT id, name, email, created_at FROM users ORDER BY created_at DESC LIMIT 100",
            &[],
        )
        .await
        .map_err(|e| {
            debug!(error = %e, "db list failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "internal server error",
                }),
            )
        })?;

    Ok(Json(rows.iter().map(row_to_user).collect()))
}

async fn delete_user(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let client = state.pool.get().await.map_err(|e| {
        debug!(error = %e, "db pool error");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "internal server error",
            }),
        )
    })?;

    let affected = client
        .execute("DELETE FROM users WHERE id = $1", &[&id])
        .await
        .map_err(|e| {
            debug!(error = %e, %id, "db delete failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "internal server error",
                }),
            )
        })?;

    if affected == 0 {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "user not found",
            }),
        ));
    }

    Ok(StatusCode::NO_CONTENT)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://bench:bench@localhost:5432/bench".to_string());

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse()
        .expect("PORT must be a number");

    let pg_config: tokio_postgres::Config = database_url.parse().expect("Invalid DATABASE_URL");
    let mgr = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(mgr)
        .max_size(20)
        .build()
        .expect("Failed to create connection pool");

    let client = pool
        .get()
        .await
        .expect("Failed to connect to database");
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
            &[],
        )
        .await
        .expect("Failed to run migrations");

    let state = AppState { pool };

    let app = Router::new()
        .route("/health", get(health))
        .route("/users", post(create_user).get(list_users))
        .route("/users/{id}", get(get_user).delete(delete_user))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!(%addr, "rust-axum listening");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
