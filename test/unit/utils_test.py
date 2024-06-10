from pudl.utils import retry


def test_retry(mocker):
    func = mocker.MagicMock(side_effect=[RuntimeError, RuntimeError, RuntimeError, 1])
    sleep_mock = mocker.MagicMock()
    with mocker.patch("time.sleep", sleep_mock):
        assert retry(func=func, retry_on=(RuntimeError,)) == 1

    assert sleep_mock.call_count == 3
    sleep_mock.assert_has_calls([mocker.call(2**x) for x in range(3)])
