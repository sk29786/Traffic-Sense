# Overview

This is a real-time traffic simulation system that generates, analyzes, and visualizes vehicle traffic data. The system consists of a traffic simulation engine that creates realistic vehicle movements, an analytics module for detecting congestion patterns, and a Streamlit-based dashboard for data visualization. The application simulates various vehicle types (cars, trucks, buses, motorcycles) moving along predefined routes, stores the data in PostgreSQL, and provides real-time insights into traffic congestion patterns.

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Data Layer
- **Database**: PostgreSQL for persistent storage of vehicle positions, routes, and congestion data
- **Data Models**: Dataclass-based models for Vehicle, Route, and CongestionPoint entities
- **Database Manager**: Centralized connection handling with environment variable configuration

## Simulation Engine
- **Traffic Simulation**: Core engine generating realistic vehicle movements with configurable parameters
- **Vehicle Generation**: Weighted random vehicle creation with realistic speed and type distributions
- **Route System**: Multi-route simulation with dynamic vehicle spawning and despawning
- **Threading**: Separate simulation thread for non-blocking real-time data generation

## Analytics Engine
- **Congestion Detection**: Algorithm analyzing vehicle density and speed to identify traffic bottlenecks
- **Metrics Calculation**: Speed thresholds and area-based congestion analysis
- **Data Processing**: Pandas-based data analysis for traffic pattern recognition

## Presentation Layer
- **Streamlit Dashboard**: Web-based interface for real-time visualization
- **Interactive Charts**: Plotly and Matplotlib integration for dynamic traffic visualization
- **Resource Caching**: Streamlit caching for optimized system initialization

## Configuration Management
- Environment variables for database connectivity (PGHOST, PGDATABASE, etc.)
- Alternative DATABASE_URL support for deployment flexibility
- Configurable simulation parameters (spawn rates, thresholds, vehicle limits)

# External Dependencies

## Database
- **PostgreSQL**: Primary data storage with psycopg2 driver
- **Connection Methods**: Environment variables or DATABASE_URL configuration

## Data Processing
- **Pandas**: DataFrame operations for traffic data analysis
- **NumPy**: Numerical computations for analytics calculations

## Visualization
- **Streamlit**: Web dashboard framework for real-time data display
- **Plotly**: Interactive charting for traffic visualization
- **Matplotlib/Seaborn**: Statistical plotting for analytics insights

## Development
- **Python Standard Library**: Threading, datetime, logging, and data structures
- **Dataclasses**: Type-safe model definitions