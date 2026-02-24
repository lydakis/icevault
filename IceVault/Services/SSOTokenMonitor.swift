import Foundation
@preconcurrency import UserNotifications

@MainActor
final class SSOTokenMonitor: NSObject, ObservableObject {
    typealias SSOLoginRunner = @Sendable (_ profileName: String) -> Int32
    typealias MonitoringTimerFactory = @MainActor (
        _ interval: TimeInterval,
        _ repeats: Bool,
        _ block: @escaping @Sendable (Timer) -> Void
    ) -> Timer

    @Published private(set) var status: SSOTokenStatus?
    @Published private(set) var sessionExpiry: Date?
    @Published private(set) var notificationsAuthorized: Bool
    @Published private(set) var lastCheckedAt: Date?

    private let fileManager: FileManager
    private let userDefaults: UserDefaults
    private let ssoLoginRunner: SSOLoginRunner
    private let monitoringTimerFactory: MonitoringTimerFactory

    private var monitoredProfileName: String?
    nonisolated(unsafe) private var monitorTimer: Timer?
    private var loginTask: Task<Void, Never>?
    private var isRequestingAuthorization = false

    nonisolated private static let checkInterval: TimeInterval = 60 * 60
    nonisolated private static let expiringSoonThreshold: TimeInterval = 24 * 60 * 60
    nonisolated private static let notificationCategoryIdentifier = "ICEVAULT_SSO_EXPIRY"
    nonisolated private static let refreshActionIdentifier = "ICEVAULT_SSO_REFRESH_LOGIN"
    nonisolated private static let notificationActionUserInfoKey = "icevaultAction"
    nonisolated private static let notificationProfileUserInfoKey = "profileName"
    nonisolated private static let notificationRefreshAction = "refreshSSOLogin"
    nonisolated private static let lastSoonEventKey = "IceVault.ssoMonitor.lastSoonEvent"
    nonisolated private static let lastSoonDateKey = "IceVault.ssoMonitor.lastSoonDate"
    nonisolated private static let lastExpiredEventKey = "IceVault.ssoMonitor.lastExpiredEvent"
    nonisolated private static let lastExpiredDateKey = "IceVault.ssoMonitor.lastExpiredDate"
    nonisolated private static let supportsUserNotifications = {
        let mainBundle = Bundle.main
        guard mainBundle.bundleURL.pathExtension.caseInsensitiveCompare("app") == .orderedSame else {
            return false
        }

        let packageType = mainBundle.object(forInfoDictionaryKey: "CFBundlePackageType") as? String
        return packageType == "APPL"
    }()

    init(
        fileManager: FileManager = .default,
        userDefaults: UserDefaults = .standard,
        autoStart: Bool = true,
        ssoLoginRunner: @escaping SSOLoginRunner = SSOTokenMonitor.runSSOLoginProcess,
        monitoringTimerFactory: @escaping MonitoringTimerFactory = { interval, repeats, block in
            Timer(timeInterval: interval, repeats: repeats, block: block)
        }
    ) {
        self.fileManager = fileManager
        self.userDefaults = userDefaults
        self.ssoLoginRunner = ssoLoginRunner
        self.monitoringTimerFactory = monitoringTimerFactory
        self.status = nil
        self.sessionExpiry = nil
        self.notificationsAuthorized = false
        self.lastCheckedAt = nil
        self.monitoredProfileName = nil
        super.init()

        if autoStart {
            configureNotificationCenter()
            startMonitoringTimer()

            Task { [weak self] in
                await self?.refreshNotificationAuthorizationStatus()
            }
        }
    }

    deinit {
        let timer = monitorTimer
        if Thread.isMainThread {
            timer?.invalidate()
        } else {
            DispatchQueue.main.async {
                timer?.invalidate()
            }
        }
    }

    func configure(profileName: String?) {
        let normalizedProfileName = Self.trimmed(profileName)
        monitoredProfileName = normalizedProfileName.isEmpty ? nil : normalizedProfileName
        refreshNow()
    }

    func requestNotificationPermissionIfNeeded() {
        guard Self.supportsUserNotifications else {
            notificationsAuthorized = false
            return
        }

        Task { [weak self] in
            guard let self else {
                return
            }

            let center = UNUserNotificationCenter.current()
            let settings = await center.notificationSettings()

            switch settings.authorizationStatus {
            case .authorized, .provisional:
                notificationsAuthorized = true
            case .notDetermined:
                guard !isRequestingAuthorization else {
                    return
                }

                isRequestingAuthorization = true
                let granted = (try? await center.requestAuthorization(options: [.alert, .sound, .badge])) ?? false
                notificationsAuthorized = granted
                isRequestingAuthorization = false
            case .denied:
                notificationsAuthorized = false
            @unknown default:
                notificationsAuthorized = false
            }
        }
    }

    func refreshNow(now: Date = Date()) {
        lastCheckedAt = now

        guard let profileName = monitoredProfileName else {
            status = nil
            sessionExpiry = nil
            return
        }

        guard let profile = GlacierClient.ssoProfile(named: profileName, fileManager: fileManager) else {
            status = .missingProfile
            sessionExpiry = nil
            return
        }

        let ssoExpiry = latestSSOTokenExpiry(matchingStartURL: profile.startURL)
        let roleCredentialExpiry = latestCLIRoleCredentialExpiry(matchingStartURL: profile.startURL)
        let effectiveExpiry = ssoExpiry ?? roleCredentialExpiry

        sessionExpiry = effectiveExpiry

        guard let effectiveExpiry else {
            status = .missingToken
            return
        }

        if effectiveExpiry <= now {
            status = .expired(expiresAt: effectiveExpiry)
        } else {
            status = .valid(expiresAt: effectiveExpiry)
        }

        maybeNotifyForSessionState(profileName: profileName, expiresAt: effectiveExpiry, now: now)
    }

    func isSessionValid(now: Date = Date()) -> Bool {
        guard let status else {
            return false
        }

        switch status {
        case .valid(let expiresAt):
            return expiresAt > now
        case .expired, .missingProfile, .missingToken:
            return false
        }
    }

    func sessionExpiresAt() -> Date? {
        sessionExpiry
    }

    func timeUntilExpiry(now: Date = Date()) -> TimeInterval? {
        guard let expiresAt = sessionExpiry else {
            return nil
        }

        return expiresAt.timeIntervalSince(now)
    }

    func refreshLogin() {
        runSSOLogin(profileNameOverride: nil)
    }

    private func startMonitoringTimer() {
        monitorTimer?.invalidate()
        let timer = monitoringTimerFactory(Self.checkInterval, true) { [weak self] _ in
            Task { @MainActor [weak self] in
                self?.refreshNow()
            }
        }
        monitorTimer = timer
        RunLoop.main.add(timer, forMode: .common)
    }

    private func configureNotificationCenter() {
        guard Self.supportsUserNotifications else {
            // Running outside a proper .app bundle (e.g. swift run) — all notifications are a no-op.
            return
        }
        let center = UNUserNotificationCenter.current()
        center.delegate = self

        let refreshAction = UNNotificationAction(
            identifier: Self.refreshActionIdentifier,
            title: "Refresh Login",
            options: [.foreground]
        )
        let category = UNNotificationCategory(
            identifier: Self.notificationCategoryIdentifier,
            actions: [refreshAction],
            intentIdentifiers: [],
            options: []
        )
        center.setNotificationCategories([category])
    }

    private func refreshNotificationAuthorizationStatus() async {
        guard Self.supportsUserNotifications else {
            notificationsAuthorized = false
            return
        }

        let settings = await UNUserNotificationCenter.current().notificationSettings()
        notificationsAuthorized = Self.isNotificationAuthorized(settings.authorizationStatus)
    }

    private static func isNotificationAuthorized(_ status: UNAuthorizationStatus) -> Bool {
        switch status {
        case .authorized, .provisional:
            return true
        case .notDetermined, .denied:
            return false
        @unknown default:
            return false
        }
    }

    private func maybeNotifyForSessionState(profileName: String, expiresAt: Date, now: Date) {
        guard notificationsAuthorized else {
            return
        }

        let eventKey = notificationEventKey(profileName: profileName, expiresAt: expiresAt)
        switch status {
        case .valid:
            let remaining = expiresAt.timeIntervalSince(now)
            guard remaining > 0, remaining < Self.expiringSoonThreshold else {
                return
            }

            if userDefaults.string(forKey: Self.lastSoonEventKey) == eventKey {
                return
            }

            postNotification(
                identifier: notificationIdentifier(prefix: "soon", profileName: profileName, expiresAt: expiresAt),
                title: "IceVault: AWS login expires soon — click to refresh",
                body: "Profile '\(profileName)' expires at \(expiresAt.formatted(date: .abbreviated, time: .shortened)).",
                profileName: profileName
            )
            userDefaults.set(eventKey, forKey: Self.lastSoonEventKey)
            userDefaults.set(now, forKey: Self.lastSoonDateKey)
        case .expired:
            if userDefaults.string(forKey: Self.lastExpiredEventKey) == eventKey {
                return
            }

            postNotification(
                identifier: notificationIdentifier(prefix: "expired", profileName: profileName, expiresAt: expiresAt),
                title: "IceVault: AWS login expired — backup paused until you re-authenticate",
                body: "Profile '\(profileName)' expired at \(expiresAt.formatted(date: .abbreviated, time: .shortened)).",
                profileName: profileName
            )
            userDefaults.set(eventKey, forKey: Self.lastExpiredEventKey)
            userDefaults.set(now, forKey: Self.lastExpiredDateKey)
        case .missingProfile, .missingToken, .none:
            break
        }
    }

    private func notificationEventKey(profileName: String, expiresAt: Date) -> String {
        "\(profileName.lowercased())|\(Int(expiresAt.timeIntervalSince1970))"
    }

    private func notificationIdentifier(prefix: String, profileName: String, expiresAt: Date) -> String {
        let normalizedProfileName = profileName.replacingOccurrences(
            of: "[^A-Za-z0-9._-]",
            with: "_",
            options: .regularExpression
        )
        return "icevault.sso.\(prefix).\(normalizedProfileName).\(Int(expiresAt.timeIntervalSince1970))"
    }

    private func postNotification(
        identifier: String,
        title: String,
        body: String,
        profileName: String
    ) {
        guard Self.supportsUserNotifications else {
            return
        }

        let content = UNMutableNotificationContent()
        content.title = title
        content.body = body
        content.sound = .default
        content.categoryIdentifier = Self.notificationCategoryIdentifier
        content.userInfo = [
            Self.notificationActionUserInfoKey: Self.notificationRefreshAction,
            Self.notificationProfileUserInfoKey: profileName
        ]

        let request = UNNotificationRequest(
            identifier: identifier,
            content: content,
            trigger: nil
        )
        UNUserNotificationCenter.current().add(request)
    }

    private func runSSOLogin(profileNameOverride: String?) {
        let profileName = Self.trimmed(profileNameOverride ?? monitoredProfileName)
        guard !profileName.isEmpty else {
            return
        }

        guard loginTask == nil else {
            return
        }

        let loginRunner = ssoLoginRunner
        loginTask = Task.detached(priority: .userInitiated) { [profileName] in
            _ = loginRunner(profileName)
            await MainActor.run { [weak self] in
                self?.loginTask = nil
                self?.refreshNow()
            }
        }
    }

    nonisolated private static func runSSOLoginProcess(profileName: String) -> Int32 {
        let process = Process()
        let environment = AWSCLI.makeEnvironment(
            base: ProcessInfo.processInfo.environment,
            profileName: profileName
        )
        guard let awsExecutableURL = AWSCLI.executableURL(environment: environment) else {
            return -1
        }

        process.executableURL = awsExecutableURL
        process.arguments = ["sso", "login", "--profile", profileName]
        process.environment = environment

        process.standardOutput = Pipe()
        process.standardError = Pipe()

        do {
            try process.run()
        } catch {
            return -1
        }

        process.waitUntilExit()
        return process.terminationStatus
    }

    private func latestSSOTokenExpiry(matchingStartURL startURL: String) -> Date? {
        let cacheDirectory = awsDirectoryURL()
            .appendingPathComponent("sso", isDirectory: true)
            .appendingPathComponent("cache", isDirectory: true)

        let entries = cacheEntries(in: cacheDirectory)
            .compactMap(parseSSOTokenCacheEntry(from:))

        let normalizedStartURL = Self.trimmed(startURL)
        guard !normalizedStartURL.isEmpty else {
            return nil
        }

        return entries
            .filter { $0.startURL.caseInsensitiveCompare(normalizedStartURL) == .orderedSame }
            .map(\.expiresAt)
            .max()
    }

    private func latestCLIRoleCredentialExpiry(matchingStartURL startURL: String) -> Date? {
        let cacheDirectory = awsDirectoryURL()
            .appendingPathComponent("cli", isDirectory: true)
            .appendingPathComponent("cache", isDirectory: true)

        let entries = cacheEntries(in: cacheDirectory)
            .compactMap(parseCLIRoleCredentialCacheEntry(from:))

        let normalizedStartURL = Self.trimmed(startURL)
        guard !normalizedStartURL.isEmpty else {
            return entries.map(\.expiration).max()
        }

        let matchingEntries = entries.filter { entry in
            guard let entryStartURL = entry.startURL else {
                return true
            }
            return entryStartURL.caseInsensitiveCompare(normalizedStartURL) == .orderedSame
        }

        return matchingEntries.map(\.expiration).max()
    }

    private func cacheEntries(in directoryURL: URL) -> [URL] {
        guard let files = try? fileManager.contentsOfDirectory(
            at: directoryURL,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else {
            return []
        }

        return files.filter { $0.pathExtension.caseInsensitiveCompare("json") == .orderedSame }
    }

    private func parseSSOTokenCacheEntry(from fileURL: URL) -> SSOTokenCacheEntry? {
        guard
            let json = jsonDictionary(from: fileURL),
            let startURL = nonEmptyString(forKeys: ["startUrl", "start_url"], in: json),
            let accessToken = nonEmptyString(forKeys: ["accessToken", "access_token"], in: json),
            let expiresAtRaw = nonEmptyString(forKeys: ["expiresAt", "expires_at"], in: json),
            let expiresAt = Self.parseDate(expiresAtRaw)
        else {
            return nil
        }

        return SSOTokenCacheEntry(
            startURL: startURL,
            accessToken: accessToken,
            expiresAt: expiresAt
        )
    }

    private func parseCLIRoleCredentialCacheEntry(from fileURL: URL) -> CLIRoleCredentialCacheEntry? {
        guard let json = jsonDictionary(from: fileURL) else {
            return nil
        }

        let credentialsNode = json["Credentials"] as? [String: Any]
        let expirationRaw = nonEmptyString(forKeys: ["Expiration", "expiration"], in: json)
            ?? nonEmptyString(forKeys: ["Expiration", "expiration"], in: credentialsNode)
        guard let expiration = Self.parseDate(expirationRaw) else {
            return nil
        }

        let startURL = nonEmptyString(forKeys: ["startUrl", "start_url", "StartUrl"], in: json)
            ?? nonEmptyString(forKeys: ["startUrl", "start_url", "StartUrl"], in: credentialsNode)

        return CLIRoleCredentialCacheEntry(
            startURL: startURL,
            expiration: expiration
        )
    }

    private func jsonDictionary(from fileURL: URL) -> [String: Any]? {
        guard
            let data = try? Data(contentsOf: fileURL),
            let jsonObject = try? JSONSerialization.jsonObject(with: data),
            let json = jsonObject as? [String: Any]
        else {
            return nil
        }

        return json
    }

    private func nonEmptyString(forKeys keys: [String], in dictionary: [String: Any]?) -> String? {
        guard let dictionary else {
            return nil
        }

        for key in keys {
            let value = Self.trimmed(dictionary[key] as? String)
            if !value.isEmpty {
                return value
            }
        }

        return nil
    }

    private static func parseDate(_ value: String?) -> Date? {
        let normalizedValue = trimmed(value)
        guard !normalizedValue.isEmpty else {
            return nil
        }

        let iso8601WithFractionalSeconds = ISO8601DateFormatter()
        iso8601WithFractionalSeconds.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = iso8601WithFractionalSeconds.date(from: normalizedValue) {
            return date
        }

        let iso8601 = ISO8601DateFormatter()
        iso8601.formatOptions = [.withInternetDateTime]
        if let date = iso8601.date(from: normalizedValue) {
            return date
        }

        let awsUTCDateFormatter = DateFormatter()
        awsUTCDateFormatter.locale = Locale(identifier: "en_US_POSIX")
        awsUTCDateFormatter.timeZone = TimeZone(secondsFromGMT: 0)
        awsUTCDateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'UTC'"
        if let date = awsUTCDateFormatter.date(from: normalizedValue) {
            return date
        }

        let awsOffsetDateFormatter = DateFormatter()
        awsOffsetDateFormatter.locale = Locale(identifier: "en_US_POSIX")
        awsOffsetDateFormatter.timeZone = TimeZone(secondsFromGMT: 0)
        awsOffsetDateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZ"
        return awsOffsetDateFormatter.date(from: normalizedValue)
    }

    private func awsDirectoryURL() -> URL {
        fileManager.homeDirectoryForCurrentUser
            .appendingPathComponent(".aws", isDirectory: true)
    }

    private static func trimmed(_ value: String?) -> String {
        (value ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

extension SSOTokenMonitor: UNUserNotificationCenterDelegate {
    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse
    ) async {
        guard
            let action = response.notification.request.content.userInfo[Self.notificationActionUserInfoKey] as? String,
            action == Self.notificationRefreshAction
        else {
            return
        }

        let actionIdentifier = response.actionIdentifier
        guard
            actionIdentifier == UNNotificationDefaultActionIdentifier
                || actionIdentifier == Self.refreshActionIdentifier
        else {
            return
        }

        let profileName = response.notification.request.content.userInfo[Self.notificationProfileUserInfoKey] as? String
        await MainActor.run { [weak self] in
            self?.runSSOLogin(profileNameOverride: profileName)
        }
    }

    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification
    ) async -> UNNotificationPresentationOptions {
        [.banner, .sound]
    }
}

private struct SSOTokenCacheEntry: Sendable {
    let startURL: String
    let accessToken: String
    let expiresAt: Date
}

private struct CLIRoleCredentialCacheEntry: Sendable {
    let startURL: String?
    let expiration: Date
}
