# DWCS

> **Distributed World Computation & Synchronization**

DWCS is an experimental, engine-agnostic backend for coordinating distributed computation and synchronizing a shared world state across connected clients.

Unlike traditional multiplayer backends that execute all gameplay computation on the server, DWCS is designed around a different responsibility model. The backend does not perform application-specific computation. Instead, it coordinates ownership, accepts computation results from clients or peers, merges those results into a shared world state, and distributes synchronized updates back to participants.

DWCS is intentionally data-agnostic. It does not know what your data represents. To the backend, every payload is simply structured bytes. The meaning of those bytes, how they are generated, and how they are interpreted are entirely defined by the application using DWCS.

---

# Why This Exists

This project began as part of the research and development for a future open-world VR game.

One of the major challenges in VR is balancing visual fidelity, large persistent worlds, and hardware limitations. Standalone VR devices have significantly less computing power than desktop systems, while traditional server-authoritative architectures often require expensive server-side computation to simulate large dynamic worlds.

The goal behind DWCS was to explore a different architecture where:

* clients perform application-specific computation,
* the backend coordinates and synchronizes results,
* the server remains lightweight,
* developers retain full control over validation and game logic.

Although DWCS was originally created for that future project, the synchronization backend itself is independent of any particular game or engine and has been released as open source so others can experiment with, improve, or adapt the architecture for their own projects.

---

# Project Status

**Current Status:** Experimental Prototype

DWCS implements the core coordination and synchronization architecture but is not intended to be considered production-ready.

Protocols, APIs, and internal behavior may change significantly as the project evolves.

---

# Core Philosophy

DWCS follows a simple principle:

> The backend coordinates computation. It does not perform computation.

Application logic belongs to the application.

Game rules belong to the game.

Validation belongs to the developer.

DWCS only coordinates how data moves through the system.

---

# What DWCS Does

DWCS provides infrastructure for:

* distributed task coordination
* shared world synchronization
* ownership and lease management (server mode)
* distributed peer synchronization (peer mode)
* versioned world state updates
* conflict resolution
* task distribution
* world projection
* subscription filtering
* tag-based routing
* metrics and observability
* transport abstraction
* merge pipeline integration

The backend maintains synchronized world state while remaining completely unaware of what the data actually represents.

---

# What DWCS Does NOT Do

DWCS intentionally does **not** provide:

* game logic
* rendering
* physics
* animation systems
* AI
* scripting
* engine integration
* persistence
* matchmaking
* authentication
* anti-cheat
* world generation
* asset streaming

Those responsibilities belong to the application built on top of DWCS.

---

# Architecture

DWCS currently supports two deployment models.

## Server Mode

A central server maintains the authoritative world state.

Clients request work, submit results, and receive synchronized updates from the server.

This mode provides:

* authoritative coordination
* ownership tracking
* lease management
* centralized synchronization
* metrics
* observability

---

## Peer Mode

No central authority exists.

Each participant computes locally, synchronizes through gossip, and converges toward a shared world state using version-based conflict resolution.

This mode favors decentralization over strong consistency.

---

# Computation and Synchronization

DWCS separates two independent concepts.

### Domain 1 — Computation

Determines what a participant is allowed to compute.

How work is selected is entirely application-defined.

DWCS only distributes and coordinates available work.

---

### Domain 2 — Synchronization

Determines which synchronized world updates are delivered to a participant.

Synchronization visibility is independent from computation ownership.

An application may choose to compute one portion of the world while observing another.

---

# Data Model

DWCS never interprets payload contents.

Every submission is treated as opaque data.

The application decides:

* payload structure
* serialization format
* validation rules
* merge behavior
* object semantics

The backend only coordinates transport, synchronization, ownership, and versioning.

---

# Intended Use Cases

DWCS may be useful for applications that require:

* persistent shared worlds
* distributed computation
* synchronized simulations
* multiplayer world state
* collaborative simulations
* experimental networking research
* large-scale synchronization systems

It is not limited to games.

Any application requiring coordinated distributed state may build on top of DWCS.

---

# Open Source

DWCS is released as open source to encourage experimentation, learning, and community contributions.

The architecture is still evolving, and improvements, discussion, and alternative implementations are welcome.

If you build something interesting with DWCS, consider sharing it with the community.

---

# License

DWCS is licensed under the **GNU General Public License v3.0 (GPL-3.0)**.

You are free to:

* use the software
* modify the software
* redistribute the software
* use it commercially under the terms of the GPL

If you distribute modified versions of DWCS, you must comply with the requirements of the GPL-3.0 license, including making the corresponding source code available under the same license.

See the `LICENSE` file for the complete license text.

---

# Documentation

Additional documentation is available in the `docs/` directory.

Recommended reading order:

* Architecture
* Protocol
* Integration Guide
* Configuration
* Roadmap

---

# Acknowledgements

DWCS was originally developed as the networking and synchronization foundation for a future open-world VR project.

The backend has been released independently so that its architecture can be explored, evaluated, and improved outside of that game.
