import { DOWNLOAD_PRIORITIES, downloadPriorityLabel } from '../src/priority';

test('defines the scheduler priority bands in ascending order', () => {
  expect(DOWNLOAD_PRIORITIES.map(({ value }) => value)).toEqual([-100, -50, 0, 50, 100, 900]);
});

test.each([
  [-100, 'Very low'],
  [-50, 'Low'],
  [0, 'Normal'],
  [50, 'High'],
  [100, 'Very high'],
  [900, 'Force'],
  [25, 'Custom +25'],
  [-25, 'Custom -25'],
])('labels priority %i as %s', (priority, expected) => {
  expect(downloadPriorityLabel(priority)).toBe(expected);
});
