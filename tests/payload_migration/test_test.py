from typing import List
from datetime import datetime
import pytest
from dataclasses import dataclass


# The code we're testing
@dataclass
class User:
    id: int
    email: str
    name: str
    created_at: datetime

    def __post_init__(self) -> None:
        """Validate email format on initialization."""
        if not self._is_valid_email(self.email):
            raise ValueError(f"Invalid email format: {self.email}")

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        """Basica email validation."""
        if not email or '@' not in email:
            return False
        local, domain = email.rsplit('@', 1)
        return bool(local and domain and '.' in domain)


class UserNotFoundError(Exception):
    pass


class UserService:
    def __init__(self) -> None:
        self._users: List[User] = []
        self._next_id: int = 1

    def create_user(self, email: str, name: str) -> User:
        """Create a new user with validated input."""
        if not name.strip():
            raise ValueError("Name cannot be empty")

        user = User(
            id=self._next_id,
            email=email,
            name=name.strip(),
            created_at=datetime.utcnow()
        )
        self._users.append(user)
        self._next_id += 1
        return user

    def get_user_by_email(self, email: str) -> User:
        """Retrieve a user by email."""
        for user in self._users:
            if user.email == email:
                return user
        raise UserNotFoundError(f"User with email {email} not found")


# Tests
@pytest.fixture
def user_service() -> UserService:
    """Provide a fresh UserService instance."""
    return UserService()


@pytest.fixture
def sample_user(user_service: UserService) -> User:
    """Provide a pre-created user."""
    return user_service.create_user("tests@example.com", "Test User")


def test_create_user_success(user_service: UserService) -> None:
    """Test successful user creation."""
    email = "john@example.com"
    name = "John Doe"

    user = user_service.create_user(email, name)

    assert user.email == email
    assert user.name == name
    assert user.id == 1
    assert isinstance(user.created_at, datetime)


def test_get_user_by_email_success(
    user_service: UserService,
    sample_user: User
) -> None:
    """Test successful user retrieval."""
    found_user = user_service.get_user_by_email(sample_user.email)
    assert found_user == sample_user


def test_get_user_by_email_not_found(user_service: UserService) -> None:
    """Test user retrieval with non-existent email."""
    with pytest.raises(UserNotFoundError) as exc_info:
        user_service.get_user_by_email("nonexistent@example.com")

    assert "not found" in str(exc_info.value)


@pytest.mark.parametrize("invalid_input", [
    ("", "Test User"),  # Empty email
    ("not_an_email", "Test User"),  # Invalid email format
    ("tests@example.com", ""),  # Empty name
    ("@missing_username.com", "Test User"),  # Invalid email
    ("missing_domain@", "Test User"),  # Invalid email
])
def test_create_user_invalid_input(
    user_service: UserService,
    invalid_input: tuple[str, str]
) -> None:
    """Test user creation with invalid inputs."""
    email, name = invalid_input
    with pytest.raises(ValueError):
        user_service.create_user(email, name)


def test_user_ids_are_unique(user_service: UserService) -> None:
    """Test that each user gets a unique ID."""
    user1 = user_service.create_user("user1@example.com", "User One")
    user2 = user_service.create_user("user2@example.com", "User Two")

    assert user1.id != user2.id


def test_user_name_is_stripped(user_service: UserService) -> None:
    """Test that whitespace is stripped from names."""
    name_with_spaces = "  John Doe  "
    user = user_service.create_user("john@example.com", name_with_spaces)

    assert user.name == name_with_spaces.strip()