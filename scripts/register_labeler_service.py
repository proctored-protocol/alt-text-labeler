from app.publisher.service_record import put_labeler_service_record


def main() -> None:
    result = put_labeler_service_record()
    print("Labeler service record written.")
    print(result)


if __name__ == "__main__":
    main()