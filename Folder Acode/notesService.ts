import { 
  collection, 
  doc, 
  setDoc, 
  deleteDoc, 
  onSnapshot, 
  writeBatch,
  Unsubscribe 
} from 'firebase/firestore';
import { db } from './config';
import { Note } from '../types';
import { handleFirestoreError, OperationType } from './errorHandler';

/**
 * Validate and sanitize note data according to the firebase-blueprint schema:
 * title max 300, content max 100000, category max 50, id max 128
 */
function sanitizeNotePayload(note: Note, userId: string): Record<string, unknown> {
  return {
    id: String(note.id || '').slice(0, 128),
    userId: String(userId).slice(0, 128),
    title: String(note.title || '').slice(0, 300),
    content: String(note.content || '').slice(0, 100000),
    isPinned: Boolean(note.isPinned),
    category: String(note.category || 'General').slice(0, 50),
    createdAt: Number(note.createdAt) || Date.now(),
    updatedAt: Number(note.updatedAt) || Date.now(),
  };
}

/**
 * Real-time listener for user notes
 */
export function subscribeToUserNotes(
  userId: string,
  onNotesUpdate: (notes: Note[]) => void,
  onError?: (error: Error) => void
): Unsubscribe {
  const notesPath = `users/${userId}/notes`;
  const notesCollection = collection(db, 'users', userId, 'notes');

  const unsubscribe = onSnapshot(
    notesCollection,
    (snapshot) => {
      const notesList: Note[] = [];
      snapshot.forEach((docSnap) => {
        const data = docSnap.data();
        notesList.push({
          id: data.id || docSnap.id,
          userId: data.userId || userId,
          title: data.title || '',
          content: data.content || '',
          isPinned: Boolean(data.isPinned),
          category: data.category || 'General',
          createdAt: Number(data.createdAt) || Date.now(),
          updatedAt: Number(data.updatedAt) || Date.now(),
        });
      });

      // Client-side sort: pinned first, then updatedAt descending
      notesList.sort((a, b) => {
        if (a.isPinned && !b.isPinned) return -1;
        if (!a.isPinned && b.isPinned) return 1;
        return b.updatedAt - a.updatedAt;
      });

      onNotesUpdate(notesList);
    },
    (error) => {
      try {
        handleFirestoreError(error, OperationType.GET, notesPath);
      } catch (err) {
        if (onError && err instanceof Error) {
          onError(err);
        } else {
          console.error('Notes subscription error:', error);
        }
      }
    }
  );

  return unsubscribe;
}

/**
 * Save or update a note in user's Firestore collection
 */
export async function saveNoteToFirestore(userId: string, note: Note): Promise<void> {
  const path = `users/${userId}/notes/${note.id}`;
  try {
    const noteRef = doc(db, 'users', userId, 'notes', note.id);
    const payload = sanitizeNotePayload(note, userId);
    await setDoc(noteRef, payload, { merge: true });
  } catch (error) {
    handleFirestoreError(error, OperationType.WRITE, path);
  }
}

/**
 * Delete a note from user's Firestore collection
 */
export async function deleteNoteFromFirestore(userId: string, noteId: string): Promise<void> {
  const path = `users/${userId}/notes/${noteId}`;
  try {
    const noteRef = doc(db, 'users', userId, 'notes', noteId);
    await deleteDoc(noteRef);
  } catch (error) {
    handleFirestoreError(error, OperationType.DELETE, path);
  }
}

/**
 * Sync initial local notes to Firestore when user first logs in
 */
export async function syncLocalNotesToFirestore(userId: string, localNotes: Note[]): Promise<void> {
  if (!localNotes.length) return;
  const path = `users/${userId}/notes`;

  try {
    const batch = writeBatch(db);
    // Limit to 20 initial notes in a batch
    const notesToSync = localNotes.slice(0, 20);
    for (const note of notesToSync) {
      const noteRef = doc(db, 'users', userId, 'notes', note.id);
      const payload = sanitizeNotePayload(note, userId);
      batch.set(noteRef, payload, { merge: true });
    }
    await batch.commit();
  } catch (error) {
    handleFirestoreError(error, OperationType.WRITE, path);
  }
}
