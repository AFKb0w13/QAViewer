# PRD: Question Area Review Web App

## Product Name
Question Area Review Web App

## Purpose
Build a web-based GIS viewer for reviewing **question areas**. A question area is a place where two GIS layers that should generally match do not match, and that difference needs to be reviewed.

One example is:
- a client ownership layer
- versus property tax parcel mapping

If there is a clear mismatch between the two, that location becomes a **question area**.

## Goal
Create a web application where users can:
- view question areas on a map
- search for and open a question area
- review related parcel and layer information
- see notes and details in a side panel
- discuss the issue with others
- attach and organize related documents

## Core Idea
The application should be built around the **GIS data and GIS standards provided at the beginning of the project**.

The GIS data model should help drive:
- the database structure
- the map layers
- the search fields
- the question area records
- the review workflow

## Main Users
- Internal GIS staff
- Project reviewers
- Clients
- Attorneys
- Other external stakeholders

## Main Features

### 1. Map Viewer
The site should include a map viewer built with **Leaflet**.

The map should allow users to:
- view question areas
- view parcel data
- view source comparison layers
- click a question area to open its details
- zoom to selected features
- turn layers on and off

### 2. Search and Selection
Users should be able to search by:
- question area ID
- parcel ID
- owner name
- address
- project name
- keyword

When a result is selected, the map should zoom to that location and open the related details.

### 3. Details Panel
When a question area is selected, a panel should show:
- question area ID
- status
- summary/description
- related parcels
- source layers involved
- notes
- assigned reviewer
- comments
- documents

### 4. Comments / Discussion
Users should be able to:
- add comments
- review existing comments
- discuss the issue tied to the question area

### 5. Documents
Users should be able to:
- upload documents
- view document lists
- organize documents by question area
- download documents

## Technical Direction
- Front end: Leaflet-based web app
- Back end: API layer + PostGIS
- Prototype environment: Docker-based setup for local development and testing

## Important Note
The application should **not** connect directly from the browser to PostGIS. A simple backend/API layer should sit between the front end and the database.

The front end should control:
- symbology
- popups
- map behavior
- layer visibility

## Data Assumption
The project will start with GIS data already prepared and provided as part of the build.

That data should include the main layers needed for:
- parcels
- source comparison layers
- question areas
- related attributes

The application should follow the GIS standards provided with that data.

## Prototype / Development Approach
For the prototype, the system should be set up to run in **Docker** first.

The goal is to make the initial build:
- easy to stand up
- consistent across environments
- easier to hand off
- easier to test locally

The prototype should ideally use Docker for:
- the web application
- the backend/API
- the PostGIS database

## MVP Scope
The first version should include:
- login/access control
- map viewer
- question area layer
- parcel layer
- search
- details panel
- comments
- document upload/list
- basic status tracking

## Out of Scope for First Version
- advanced editing of geometry in the browser
- real-time chat
- advanced reporting
- highly complex document management
- server-side map styling tools like GeoServer

## Success Criteria
The app is successful if users can:
- quickly find a question area
- understand the map discrepancy
- review related parcel and source data
- leave comments
- attach documents
- keep the review process in one place

## Simple Product Statement
This product is a web-based GIS review tool for managing **question areas**, which are mapped locations where two GIS layers do not align as expected. The application uses Leaflet on the front end, PostGIS on the back end, and is built around GIS data and standards provided at the start of the project. For the prototype, the system should be containerized with Docker so it can be stood up quickly and consistently during early development.

