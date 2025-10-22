# MiniGCS: Distributed Object Storage System

**Technologies:** Python, FastAPI, SQLite, Docker, Reed-Solomon Erasure Coding, REST API

**Duration:** October 2024

## Project Overview
Designed and implemented a production-ready distributed object storage system with erasure coding and multi-region replication, achieving 100% success rate across 2,000 operations with sub-4ms average latency.

## Key Achievements

• **Architecture Design**: Built scalable distributed storage system with 3-node cluster architecture supporting multi-region replication across simulated geographical regions

• **Erasure Coding Implementation**: Developed Reed-Solomon erasure coding system (k=2, m=1) for data protection and fault tolerance, enabling recovery from node failures

• **REST API Development**: Created comprehensive FastAPI-based REST API with endpoints for object upload/download/list/delete operations, cluster management, and metrics collection

• **Database Management**: Implemented SQLite-based metadata service with WAL mode for concurrent access, managing object metadata, shard locations, and version tracking

• **Performance Optimization**: Achieved exceptional performance metrics with 1.08ms average download latency and 3.02ms average upload latency across 1,000 objects

• **Fault Tolerance**: Built robust node failure detection and recovery mechanisms with automatic shard repair and data reconstruction capabilities

• **Metrics & Monitoring**: Implemented comprehensive metrics collection system tracking latency, throughput, MTTR, and success rates with CSV export functionality

• **Containerization**: Created Docker and Docker Compose configurations for scalable deployment and containerized microservices architecture

• **Testing & Validation**: Developed comprehensive benchmark test suite validating system performance, data integrity, and fault tolerance across 2,000 operations

• **Documentation**: Created professional documentation including API specifications, deployment guides, and performance benchmarks

## Technical Skills Demonstrated

• **Backend Development**: Python, FastAPI, async/await programming, RESTful API design
• **Database Systems**: SQLite, metadata management, concurrent access handling
• **Distributed Systems**: Multi-node coordination, consistent hashing, replication strategies
• **Data Protection**: Reed-Solomon erasure coding, Galois field arithmetic, shard repair algorithms
• **DevOps**: Docker containerization, Docker Compose orchestration, deployment automation
• **Performance Engineering**: Latency optimization, throughput analysis, metrics collection
• **System Design**: Scalable architecture, fault tolerance, high availability systems

## Results

• **100% Success Rate**: All 2,000 test operations completed successfully
• **High Performance**: Sub-4ms average latency for all operations
• **Fault Tolerance**: Successful recovery from simulated node failures
• **Scalability**: Handled 1,000 objects with efficient erasure coding
• **Production Ready**: Enterprise-grade system with comprehensive monitoring

## Repository
GitHub: https://github.com/suhasramanand/minigcs
