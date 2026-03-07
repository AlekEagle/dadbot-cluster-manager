// We require the users.json so that typescript doesn't try to coerce the JSON file's types onto the functions in this file, which would cause errors.
const Users = require('../../config/users.json');

export function authenticate(token: string) {
  console.debug('Authenticating token:', token);
  // Is the token in the correct format?
  if (token.match(/^[A-Za-z0-9+/]+={0,2}\.[A-Za-z0-9+/]+={0,2}$/) === null) {
    console.error('Invalid token format!');
    return null;
  }
  console.debug('Token format is valid, decoding token...');
  // Decode the token and check if it matches any user in the users.json file.
  let [username, password] = token
    .split('.')
    .map((part) => Buffer.from(part, 'base64').toString('utf-8'));

  console.debug('Decoded token:', { username, password });

  // Check if the username exists and the password matches.
  if (Users[username] === password) {
    console.debug('Authentication successful for user:', username);
    // Return the username if the token is valid.
    return Object.keys(Users)[
      Object.keys(Users).findIndex((u) => u === username)
    ];
  } else {
    console.debug('Authentication failed for user:', username);
    return null;
  }
}

export function generateToken(user: string) {
  // Find the user in the users.json file.
  if (Object.keys(Users).findIndex((u) => u === user) === -1)
    throw new Error('User does not exist.');
  else
    // Generate a token by encoding the username and password in base64 and concatenating them with a dot.
    return `${Buffer.from(
      Object.keys(Users)[Object.keys(Users).findIndex((u) => u === user)],
    ).toString('base64')}.${Buffer.from(Users[user]).toString('base64')}`;
}
