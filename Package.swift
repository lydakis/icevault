// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "IceVault",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(name: "IceVault", targets: ["IceVault"])
    ],
    dependencies: [
        .package(url: "https://github.com/groue/GRDB.swift", from: "7.0.0"),
        .package(url: "https://github.com/awslabs/aws-sdk-swift", from: "1.0.0"),
        .package(url: "https://github.com/kishikawakatsumi/KeychainAccess", from: "4.2.2")
    ],
    targets: [
        .executableTarget(
            name: "IceVault",
            dependencies: [
                .product(name: "GRDB", package: "GRDB.swift"),
                .product(name: "AWSS3", package: "aws-sdk-swift"),
                .product(name: "KeychainAccess", package: "KeychainAccess")
            ],
            path: "IceVault",
            resources: [
                .process("Resources")
            ]
        )
    ]
)
