export interface PublishType {
    topic: "solarEvents" | "windEvents" | "hydroEvents";
    event: string;
    message: Record<string, any>;
    headers: Record<string, any>;
}