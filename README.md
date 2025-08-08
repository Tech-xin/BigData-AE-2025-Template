项目介绍
构建一套基于 Apache Spark 的批处理式智能推荐平台，从约2400万条美股历史数据中提取有效特征，构造因子模型对资产进行多维度打分与排序。平台面向机构投资者提供高性能、可扩展的智能资产筛选能力，支持对 ROI、Volatility、PE ratio 等关键指标的并行计算与规则过滤，最终输出推荐结果 Top5。
技术栈：Apache Spark｜Java｜RDD & Dataset｜分布式数据处理｜数据清洗｜因子选股｜性能调优
核心职责
主导推荐系统核心算法实现，基于 Spark 构建 end-to-end 分布式数据处理流程，涵盖特征工程、资产过滤与评分模块；
设计高可读性数据模型类（如 AssetFeatures、StockPrice 等），支持对多时间窗口数据的聚合与指标计算；
实施分布式任务性能优化，控制 driver 端数据回收策略，保证程序在 local[4] 模式下运行时长优于 30 秒；
结合 SQL 与 RDD 编程范式，实现复杂过滤逻辑（如低估值、低波动率资产筛选）与结果排序。
技术亮点
构建自定义因子选股逻辑，融合 ROI、Volatility、PE Ratio 等多维金融指标，实现类券商投顾策略的推荐机制；
灵活使用 Spark RDD 与 Dataset API，兼顾计算效率与代码结构清晰性；
通过自研时间窗口计算方法（结合 TimeUtil 工具类）精准提取近五日市场表现，提升推荐策略时效性；
使用 MapReduce 式并行计算逻辑，实现对千万级美股数据的稳定处理，具备良好的扩展性与工程稳定性。

Project Overview
Developed a scalable batch-processing asset recommendation system using Apache Spark. The system analyzes ~24 million historical US stock records, computes multi-dimensional financial indicators including ROI, Volatility, and PE ratio, and ranks assets based on customized investment heuristics. The platform outputs the top 5 recommended assets to assist institutional investors with data-driven decision-making.

Key Responsibilities
Designed and implemented the end-to-end Spark data pipeline covering feature extraction, asset filtering, and scoring components;
Developed modular data model classes (e.g., Asset, AssetFeatures, StockPrice) to enable flexible indicator computations across multiple time windows;
Integrated RDD transformations and Spark SQL to implement complex filtering and ranking logic under investment constraints;
Optimized distributed task execution, reducing runtime to under 30s in local[4] mode by minimizing driver memory pressure and avoiding data skew.
Technical Highlights
Built a custom quantitative screening engine combining ROI, Volatility, and PE ratio as core decision features;
Leveraged Spark's hybrid RDD + Dataset APIs to balance performance with code maintainability;
Implemented time windowing via utility class TimeUtil to accurately isolate recent 5-day asset performance;
Achieved efficient processing of tens of millions of records using parallelized MapReduce-style computation, demonstrating strong capability in distributed system design.
